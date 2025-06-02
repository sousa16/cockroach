package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// unexpectedKeyCheckOperation implements the checkOperation interface. It is a
// scrub check for the KV's integrity. This operation will detect:
//  1. Extraneous keys that fall between or outside index spans.
//  2. Extraneous column families (unexpected family IDs).
type unexpectedKeyCheckOperation struct {
	tableName *tree.TableName
	tableDesc catalog.TableDescriptor
	asOf      hlc.Timestamp
	run       unexpectedKeyCheckRun
}

// unexpectedKeyCheckRun contains the run-time state for unexpectedKeyCheckOperation.
type unexpectedKeyCheckRun struct {
	started  bool
	rows     []tree.Datums
	rowIndex int
}

func newUnexpectedKeyCheckOperation(
	tableName *tree.TableName,
	tableDesc catalog.TableDescriptor,
	asOf hlc.Timestamp,
) *unexpectedKeyCheckOperation {
	return &unexpectedKeyCheckOperation{
		tableName: tableName,
		tableDesc: tableDesc,
		asOf:      asOf,
	}
}

func (o *unexpectedKeyCheckOperation) Start(params runParams) error {
	ctx := params.ctx
	codec := params.ExecCfg().Codec
	db := params.ExecCfg().DB

	// Build a set of expected column family IDs.
	expectedFamilies := make(map[uint32]struct{})
	for _, fam := range o.tableDesc.GetFamilies() {
		expectedFamilies[uint32(fam.ID())] = struct{}{}
	}

	// Build a list of all valid index spans.
	var indexSpans []roachpb.Span
	indexSpans = append(indexSpans, o.tableDesc.PrimaryIndexSpan(codec))
	for _, idx := range o.tableDesc.DeletableNonPrimaryIndexes() {
		indexSpans = append(indexSpans, o.tableDesc.IndexSpan(codec, idx.GetID()))
	}

	// Create the full span covering the entire table.
	fullSpan := o.tableDesc.TableSpan(codec)

	it := db.NewIterator(ctx)
	defer it.Close()

	for it.SeekGE(fullSpan.Key); ; it.Next() {
		if !it.Valid() {
			break
		}
		key := it.Key()
		if !fullSpan.ContainsKey(key) {
			break
		}

		indexID, _, familyID, err := keys.DecodeTableKey(o.tableDesc.GetID(), key)
		if err != nil {
			// Not a valid table key — skip.
			continue
		}

		reason := ""
		inSpan := false
		for _, span := range indexSpans {
			if span.ContainsKey(key) {
				inSpan = true
				break
			}
		}

		if !inSpan {
			reason = "key not in any index span"
		} else if !isFamilyExpected(familyID, expectedFamilies) {
			reason = "unexpected column family"
		}

		if reason != "" {
			o.run.rows = append(o.run.rows, tree.Datums{
				tree.NewDString(reason),
				tree.NewDString(o.tableName.Catalog()),
				tree.NewDString(o.tableName.Table()),
				tree.NewDString(key.String()),
			})
		}
	}

	o.run.started = true
	return nil
}

func isFamilyExpected(familyID uint32, expected map[uint32]struct{}) bool {
	_, ok := expected[familyID]
	return ok
}

func (o *unexpectedKeyCheckOperation) Next(params runParams) (tree.Datums, error) {
	if o.Done(params.ctx) {
		return nil, nil
	}
	row := o.run.rows[o.run.rowIndex]
	o.run.rowIndex++
	return row, nil
}

func (o *unexpectedKeyCheckOperation) Started() bool {
	return o.run.started
}

func (o *unexpectedKeyCheckOperation) Done(_ context.Context) bool {
	return o.run.rowIndex >= len(o.run.rows)
}

func (o *unexpectedKeyCheckOperation) Close(_ context.Context) {
	o.run.rows = nil
}
