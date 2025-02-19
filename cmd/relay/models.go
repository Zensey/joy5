package main

type Endpoint struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	Enabled bool   `json:"enabled"`
}

type Restream struct {
	ID        string               `yaml:"id"`
	Name      string               `yaml:"name"`
	Endpoints map[string]*Endpoint `yaml:"endpoints"`
}
