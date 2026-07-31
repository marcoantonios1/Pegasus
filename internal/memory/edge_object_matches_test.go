package memory

import (
	"testing"

	"github.com/google/uuid"
)

func TestEdgeObjectMatches(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	lit1 := "olives"
	lit2 := "cilantro"

	cases := []struct {
		name   string
		edge   *Edge
		objID  *uuid.UUID
		objLit *string
		want   bool
	}{
		{"matching object_id", &Edge{ObjectID: &id1}, &id1, nil, true},
		{"different object_id", &Edge{ObjectID: &id1}, &id2, nil, false},
		{"matching object_literal", &Edge{ObjectLiteral: &lit1}, nil, &lit1, true},
		{"different object_literal", &Edge{ObjectLiteral: &lit1}, nil, &lit2, false},
		{"edge has object_id, query has literal", &Edge{ObjectID: &id1}, nil, &lit1, false},
		{"edge has literal, query has object_id", &Edge{ObjectLiteral: &lit1}, &id1, nil, false},
		{"both query fields nil", &Edge{ObjectID: &id1}, nil, nil, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := edgeObjectMatches(c.edge, c.objID, c.objLit)
			if got != c.want {
				t.Errorf("edgeObjectMatches() = %v, want %v", got, c.want)
			}
		})
	}
}
