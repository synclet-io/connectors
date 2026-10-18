package main

import airbyte "github.com/saturn4er/airbyte-go-sdk"

// credentialsSpec returns the oneOf-based credentials property spec
// matching the original Airbyte Google Sheets connector schema.
func credentialsSpec() airbyte.PropertySpec {
	return airbyte.PropertySpec{
		Title:       "Authentication",
		Description: "Credentials for connecting to the Google Sheets API",
		PropertyType: airbyte.PropertyType{
			Type: []airbyte.PropType{airbyte.Object},
		},
		OneOf: []airbyte.PropertySpec{
			{
				Title: "Authenticate via Google (OAuth)",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"auth_type", "client_id", "client_secret", "refresh_token"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"auth_type": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "Client",
					},
					"client_id": {
						Title:       "Client ID",
						Description: "Enter your Google application's Client ID",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
					"client_secret": {
						Title:       "Client Secret",
						Description: "Enter your Google application's Client Secret",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
					"refresh_token": {
						Title:       "Refresh Token",
						Description: "Enter your Google application's refresh token",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title: "Service Account Key Authentication",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"auth_type", "service_account_info"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"auth_type": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "Service",
					},
					"service_account_info": {
						Title:       "Service Account Information",
						Description: "The JSON key of the service account to use for authorization",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title:       "Application Default Credentials",
				Description: "Use Google Cloud Application Default Credentials (no extra fields needed)",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"auth_type"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"auth_type": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "ApplicationDefault",
					},
				},
			},
		},
	}
}
