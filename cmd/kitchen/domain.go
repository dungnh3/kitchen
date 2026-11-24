package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dungnh3/kitchen/cmd/kitchen/internal/scaffold"
)

// entity names one domain resource in every form the templates need.
type entity struct {
	Kebab       string // order-item
	Name        string // Pascal singular: OrderItem
	Names       string // Pascal plural: OrderItems
	Lower       string // lowerCamel singular: orderItem
	Camels      string // lowerCamel plural, the REST collection: orderItems
	Snake       string // snake singular: order_item (files, proto fields)
	Snakes      string // snake plural: order_items (table)
	Words       string // order item
	WordsPlural string // order items
	Article     string // "a" or "an"
}

var domainRE = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z][a-z0-9]*)*$`)

// newEntity builds the names of a domain from its kebab-case singular name
// and, when plural is empty, the regular English plural.
func newEntity(name, plural string) (entity, error) {
	if !domainRE.MatchString(name) {
		return entity{}, fmt.Errorf("domain %q: use lower-case words separated by dashes, singular, for example order or order-item", name)
	}
	if plural == "" {
		plural = pluralize(name)
	}
	if !domainRE.MatchString(plural) {
		return entity{}, fmt.Errorf("-plural %q: use lower-case words separated by dashes", plural)
	}
	if plural == name {
		return entity{}, fmt.Errorf("-plural %q: must differ from the singular", plural)
	}
	e := singular(name)
	pwords := strings.Split(plural, "-")
	e.Names = pascal(pwords)
	e.Snakes = strings.Join(pwords, "_")
	e.WordsPlural = strings.Join(pwords, " ")
	e.Camels = strings.ToLower(e.Names[:1]) + e.Names[1:]
	return e, nil
}

// newName builds the singular names of a consumer or a job.
func newName(name string) (entity, error) {
	if !domainRE.MatchString(name) {
		return entity{}, fmt.Errorf("name %q: use lower-case words separated by dashes, for example order-events", name)
	}
	return singular(name), nil
}

func singular(name string) entity {
	words := strings.Split(name, "-")
	e := entity{
		Kebab: name, Name: pascal(words), Snake: strings.Join(words, "_"),
		Words: strings.Join(words, " "), Article: "a",
	}
	e.Lower = strings.ToLower(e.Name[:1]) + e.Name[1:]
	if strings.ContainsRune("aeiou", rune(name[0])) {
		e.Article = "an"
	}
	return e
}

// pluralize adds the regular English plural ending to the last word.
func pluralize(s string) string {
	switch {
	case strings.HasSuffix(s, "y") && len(s) > 1 && !strings.ContainsRune("aeiou", rune(s[len(s)-2])):
		return s[:len(s)-1] + "ies"
	case strings.HasSuffix(s, "s"), strings.HasSuffix(s, "x"), strings.HasSuffix(s, "z"),
		strings.HasSuffix(s, "ch"), strings.HasSuffix(s, "sh"):
		return s + "es"
	default:
		return s + "s"
	}
}

func pascal(words []string) string {
	var b strings.Builder
	for _, w := range words {
		b.WriteString(strings.ToUpper(w[:1]) + w[1:])
	}
	return b.String()
}

// domainData is the data of templates/domain.
type domainData struct {
	Module       string
	ProtoPackage string
	Postgres     bool
	MySQL        bool
	// Outbox writes an <Entity>Created event in the transaction of a
	// create; only the example domain of a new service has events.
	Outbox bool
	E      entity
}

// renderDomain renders templates/domain for e. migration is the version
// prefix of its migration file, for example 00001.
func renderDomain(p serviceParams, e entity, migration string, outbox bool) (scaffold.Files, error) {
	files, err := scaffold.Render(templatesFS, "templates/domain", domainData{
		Module: p.Module, ProtoPackage: p.ProtoPackage(), Postgres: p.Postgres(), MySQL: p.MySQL(), Outbox: outbox, E: e,
	})
	if err != nil {
		return nil, err
	}
	r := strings.NewReplacer(
		protoPkgDir, p.ProtoPackage(),
		"_entities_", e.Snakes,
		"_entity_", e.Snake,
		"_version_", migration,
	)
	out := scaffold.Files{}
	for rel, b := range files {
		if !p.SQL() && strings.HasPrefix(rel, "migrations/") ||
			!p.Postgres() && strings.HasPrefix(rel, "internal/store/pgstore/") ||
			!p.MySQL() && strings.HasPrefix(rel, "internal/store/mysqlstore/") {
			continue
		}
		out[r.Replace(rel)] = b
	}
	return out, nil
}
