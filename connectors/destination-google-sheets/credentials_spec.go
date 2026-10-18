package main

import synclet "github.com/synclet-io/synclet-go-sdk"

// credentialsSpec returns the oneOf-based credentials property spec
// matching the original Airbyte Google Sheets connector schema.
func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:       "Authentication",
		Description: "Credentials for connecting to the Google Sheets API",
		PropertyType: synclet.PropertyType{
			Type: []synclet.PropType{synclet.Object},
		},
		OneOf: []synclet.PropertySpec{
			{
				Title: "Authenticate via Google (OAuth)",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"auth_type", "client_id", "client_secret", "refresh_token"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "Client",
					},
					"client_id": {
						Title:       "Client ID",
						Description: "Enter your Google application's Client ID",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
					"client_secret": {
						Title:       "Client Secret",
						Description: "Enter your Google application's Client Secret",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
					"refresh_token": {
						Title:       "Refresh Token",
						Description: "Enter your Google application's refresh token",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title: "Service Account Key Authentication",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"auth_type", "service_account_info"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "Service",
					},
					"service_account_info": {
						Title:       "Service Account Information",
						Description: "The JSON key of the service account to use for authorization",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title:       "Application Default Credentials",
				Description: "Use Google Cloud Application Default Credentials (no extra fields needed)",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"auth_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "ApplicationDefault",
					},
				},
			},
		},
	}
}
