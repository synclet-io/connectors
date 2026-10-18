package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// noAssociations is the subCategory of the batch error HubSpot returns for
// a record without associations to the other object.
const noAssociations = "crm.associations.NO_ASSOCIATIONS_FOUND"

// assocPageSize is the limit of the per-record associations endpoint.
const assocPageSize = "500"

// assocRead is the read of one associations stream.
type assocRead struct {
	c    *apiClient
	p    pair
	desc synclet.StreamDescriptor
	em   *synclet.Emitter
}

type assocTo struct {
	ToObjectID       json.Number `json:"toObjectId"`
	AssociationTypes []struct {
		Category string  `json:"category"`
		TypeID   int64   `json:"typeId"`
		Label    *string `json:"label"`
	} `json:"associationTypes"`
}

// read lists the ids of a, reads their associations to b through the v4
// batch read, and follows each record's paging.next.after through the
// per-record endpoint. Any batch error but noAssociations fails the read.
func (r assocRead) read(ctx context.Context) error {
	path := "crm/v4/associations/" + r.p.a.path + "/" + r.p.b.path + "/batch/read"
	return listIDs(ctx, r.c, r.p.a, func(ids []string) error {
		inputs := make([]map[string]string, len(ids))
		for i, id := range ids {
			inputs[i] = map[string]string{"id": id}
		}
		var body struct {
			Results []struct {
				From struct {
					ID string `json:"id"`
				} `json:"from"`
				To     []assocTo `json:"to"`
				Paging paging    `json:"paging"`
			} `json:"results"`
			Errors []batchError `json:"errors"`
		}
		if err := r.c.post(ctx, path, nil, map[string]any{"inputs": inputs}, &body); err != nil {
			return err
		}
		for _, e := range body.Errors {
			if e.SubCategory != noAssociations {
				return fmt.Errorf("POST %s: %s", path, e)
			}
		}
		for _, res := range body.Results {
			if res.From.ID == "" {
				return fmt.Errorf("POST %s: result without a from id", path)
			}
			if err := r.emit(res.From.ID, res.To); err != nil {
				return err
			}
			if err := r.follow(ctx, res.From.ID, res.Paging.Next.After); err != nil {
				return err
			}
		}
		return nil
	})
}

// follow reads the further pages of one record's associations.
func (r assocRead) follow(ctx context.Context, fromID, after string) error {
	path := "crm/v4/objects/" + r.p.a.path + "/" + fromID + "/associations/" + r.p.b.path
	for after != "" {
		var page listPage[assocTo]
		if err := r.c.get(ctx, path, url.Values{"after": {after}, "limit": {assocPageSize}}, &page); err != nil {
			return err
		}
		if err := r.emit(fromID, page.Results); err != nil {
			return err
		}
		after = page.Paging.Next.After
	}
	return nil
}

// emit emits one row per association type of each target.
func (r assocRead) emit(fromID string, to []assocTo) error {
	for _, t := range to {
		if t.ToObjectID == "" {
			return fmt.Errorf("association of %s %s without a toObjectId", r.p.a.name, fromID)
		}
		for _, at := range t.AssociationTypes {
			row := map[string]any{
				"from_id": fromID, "to_id": t.ToObjectID.String(), "type_id": at.TypeID,
				"category": at.Category, "label": at.Label,
			}
			if err := r.em.Record(r.desc, row); err != nil {
				return err
			}
		}
	}
	return nil
}
