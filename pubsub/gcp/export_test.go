package gcp

import gpubsub "cloud.google.com/go/pubsub/v2"

// StopTopicForTest stops the topic handle p uses for name, as Close does,
// without marking p closed: it reproduces a Publish racing Close.
func StopTopicForTest(p *Publisher, name string) {
	t, err := p.topic(name)
	if err == nil {
		t.Stop()
	}
}

// TopicSettingsForTest returns the publish settings of p's handle for name.
func TopicSettingsForTest(p *Publisher, name string) gpubsub.PublishSettings {
	t, err := p.topic(name)
	if err != nil {
		return gpubsub.PublishSettings{}
	}
	return t.PublishSettings
}
