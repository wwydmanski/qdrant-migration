package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pterm/pterm"

	"github.com/qdrant/go-client/qdrant"

	"github.com/qdrant/migration/pkg/commons"
)

type MigrateFromQdrantCmd struct {
	Source               commons.QdrantConfig    `embed:"" prefix:"source."`
	Target               commons.QdrantConfig    `embed:"" prefix:"target."`
	Migration            commons.MigrationConfig `embed:"" prefix:"migration."`
	MaxMessageSize       int                     `help:"Maximum gRPC message size in bytes (default: 33554432 = 32MB)" default:"33554432" prefix:"source."`
	EnsurePayloadIndexes bool                    `help:"Ensure payload indexes are created" default:"true" prefix:"target."`

	sourceHost string
	sourcePort int
	sourceTLS  bool
	targetHost string
	targetPort int
	targetTLS  bool
}

func (r *MigrateFromQdrantCmd) Parse() error {
	var err error
	r.sourceHost, r.sourcePort, r.sourceTLS, err = parseQdrantUrl(r.Source.Url)
	if err != nil {
		return fmt.Errorf("failed to parse source URL: %w", err)
	}

	r.targetHost, r.targetPort, r.targetTLS, err = parseQdrantUrl(r.Target.Url)
	if err != nil {
		return fmt.Errorf("failed to parse target URL: %w", err)
	}

	return nil
}

func (r *MigrateFromQdrantCmd) Validate() error {
	return validateBatchSize(r.Migration.BatchSize)
}

func (r *MigrateFromQdrantCmd) ValidateParsedValues() error {
	if r.sourceHost == r.targetHost && r.sourcePort == r.targetPort && r.Source.Collection == r.Target.Collection {
		return fmt.Errorf("source and target collections must be different")
	}

	return nil
}

func (r *MigrateFromQdrantCmd) Run(globals *Globals) error {
	pterm.DefaultHeader.WithFullWidth().Println("Qdrant Data Migration")

	err := r.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse input: %w", err)
	}
	err = r.ValidateParsedValues()
	if err != nil {
		return fmt.Errorf("failed to validate input: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sourceClient, err := connectToQdrant(globals, r.sourceHost, r.sourcePort, r.Source.APIKey, r.sourceTLS, r.MaxMessageSize)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceClient.Close()

	targetClient, err := connectToQdrant(globals, r.targetHost, r.targetPort, r.Target.APIKey, r.targetTLS, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetClient.Close()

	err = commons.PrepareOffsetsCollection(ctx, r.Migration.OffsetsCollection, targetClient)
	if err != nil {
		return fmt.Errorf("failed to prepare migration marker collection: %w", err)
	}

	sourcePointCount, err := sourceClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Source.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in source: %w", err)
	}

	err = r.prepareTargetCollection(ctx, sourceClient, r.Source.Collection, targetClient, r.Target.Collection)
	if err != nil {
		return fmt.Errorf("error preparing target collection: %w", err)
	}

	displayMigrationStart("qdrant", r.Source.Collection, r.Target.Collection)

	err = r.migrateData(ctx, sourceClient, r.Source.Collection, targetClient, r.Target.Collection, sourcePointCount)
	if err != nil {
		return fmt.Errorf("failed to migrate data: %w", err)
	}

	targetPointCount, err := targetClient.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.Target.Collection,
		Exact:          qdrant.PtrOf(true),
	})
	if err != nil {
		return fmt.Errorf("failed to count points in target: %w", err)
	}

	pterm.Info.Printfln("Target collection has %d points\n", targetPointCount)

	return nil
}

func (r *MigrateFromQdrantCmd) prepareTargetCollection(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string) error {
	sourceCollectionInfo, err := sourceClient.GetCollectionInfo(ctx, sourceCollection)
	if err != nil {
		return fmt.Errorf("failed to get source collection info: %w", err)
	}

	if r.Migration.CreateCollection {
		targetCollectionExists, err := targetClient.CollectionExists(ctx, targetCollection)
		if err != nil {
			return fmt.Errorf("failed to check if collection exists: %w", err)
		}

		if targetCollectionExists {
			fmt.Print("\n")
			pterm.Info.Printfln("Target collection '%s' already exists. Skipping creation.", targetCollection)
		} else {
			err = targetClient.CreateCollection(ctx, &qdrant.CreateCollection{
				CollectionName:         targetCollection,
				HnswConfig:             sourceCollectionInfo.Config.GetHnswConfig(),
				WalConfig:              sourceCollectionInfo.Config.GetWalConfig(),
				OptimizersConfig:       sourceCollectionInfo.Config.GetOptimizerConfig(),
				ShardNumber:            &sourceCollectionInfo.Config.GetParams().ShardNumber,
				OnDiskPayload:          &sourceCollectionInfo.Config.GetParams().OnDiskPayload,
				VectorsConfig:          sourceCollectionInfo.Config.GetParams().VectorsConfig,
				ReplicationFactor:      sourceCollectionInfo.Config.GetParams().ReplicationFactor,
				WriteConsistencyFactor: sourceCollectionInfo.Config.GetParams().WriteConsistencyFactor,
				QuantizationConfig:     sourceCollectionInfo.Config.GetQuantizationConfig(),
				ShardingMethod:         sourceCollectionInfo.Config.GetParams().ShardingMethod,
				SparseVectorsConfig:    sourceCollectionInfo.Config.GetParams().SparseVectorsConfig,
				StrictModeConfig:       sourceCollectionInfo.Config.GetStrictModeConfig(),
			})
			if err != nil {
				return fmt.Errorf("failed to create target collection: %w", err)
			}
		}
	}

	targetCollectionInfo, err := targetClient.GetCollectionInfo(ctx, targetCollection)
	if err != nil {
		return fmt.Errorf("failed to get target collection information: %w", err)
	}

	if r.EnsurePayloadIndexes {
		for name, schemaInfo := range sourceCollectionInfo.GetPayloadSchema() {
			fieldType := getFieldType(schemaInfo.GetDataType())
			if fieldType == nil {
				continue
			}

			// if there is already an index in the target collection, skip
			if _, ok := targetCollectionInfo.GetPayloadSchema()[name]; ok {
				continue
			}

			_, err = targetClient.CreateFieldIndex(
				ctx,
				&qdrant.CreateFieldIndexCollection{
					CollectionName:   r.Target.Collection,
					FieldName:        name,
					FieldType:        fieldType,
					FieldIndexParams: schemaInfo.GetParams(),
					Wait:             qdrant.PtrOf(true),
				},
			)
			if err != nil {
				return fmt.Errorf("failed creating index on target collection: %w", err)
			}
		}
	}

	return nil
}

func getFieldType(dataType qdrant.PayloadSchemaType) *qdrant.FieldType {
	switch dataType {
	case qdrant.PayloadSchemaType_Keyword:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeKeyword)
	case qdrant.PayloadSchemaType_Integer:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeInteger)
	case qdrant.PayloadSchemaType_Float:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeFloat)
	case qdrant.PayloadSchemaType_Geo:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeGeo)
	case qdrant.PayloadSchemaType_Text:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeText)
	case qdrant.PayloadSchemaType_Bool:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeBool)
	case qdrant.PayloadSchemaType_Datetime:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeDatetime)
	case qdrant.PayloadSchemaType_Uuid:
		return qdrant.PtrOf(qdrant.FieldType_FieldTypeUuid)
	}
	return nil
}

func (r *MigrateFromQdrantCmd) migrateData(ctx context.Context, sourceClient *qdrant.Client, sourceCollection string, targetClient *qdrant.Client, targetCollection string, sourcePointCount uint64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	limit := uint32(r.Migration.BatchSize)

	var offsetId *qdrant.PointId
	offsetCount := uint64(0)

	if !r.Migration.Restart {
		id, count, err := commons.GetStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceCollection)
		if err != nil {
			return fmt.Errorf("failed to get start offset: %w", err)
		}
		offsetId = id
		offsetCount = count
	}

	bar, _ := pterm.DefaultProgressbar.WithTotal(int(sourcePointCount)).Start()
	displayMigrationProgress(bar, offsetCount)

	workers := r.Migration.ParallelUploads
	if workers < 1 {
		workers = 1
	}

	type upsertResult struct {
		seq        int
		nextOffset *qdrant.PointId
		count      int
		err        error
	}

	type upsertJob struct {
		seq        int
		points     []*qdrant.RetrievedPoint
		nextOffset *qdrant.PointId
	}

	resultCh := make(chan upsertResult, workers*2)
	jobs := make(chan upsertJob, workers*2)
	errCh := make(chan error, 1)

	var once sync.Once
	sendErr := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() {
			errCh <- err
			cancel()
		})
	}

	pending := make(map[int]upsertResult)
	var commitWg sync.WaitGroup
	commitWg.Add(1)
	go func() {
		defer commitWg.Done()
		defer close(errCh)

		nextToCommit := 0
		failed := false

		process := func(res upsertResult) {
			pending[res.seq] = res
			for {
				current, ok := pending[nextToCommit]
				if !ok {
					break
				}

				offsetCount += uint64(current.count)

				if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceCollection, current.nextOffset, offsetCount); err != nil {
					sendErr(fmt.Errorf("failed to store offset: %w", err))
					failed = true
					return
				}

				bar.Add(current.count)

				delete(pending, nextToCommit)
				nextToCommit++
			}
		}

		for res := range resultCh {
			if failed {
				continue
			}

			if res.err != nil {
				sendErr(res.err)
				failed = true
				continue
			}

			process(res)
		}

		if failed {
			return
		}

		// Process any remaining batches if the channel was closed after all results were received.
		for {
			current, ok := pending[nextToCommit]
			if !ok {
				break
			}

			offsetCount += uint64(current.count)

			if err := commons.StoreStartOffset(ctx, r.Migration.OffsetsCollection, targetClient, sourceCollection, current.nextOffset, offsetCount); err != nil {
				sendErr(fmt.Errorf("failed to store offset: %w", err))
				return
			}

			bar.Add(current.count)

			delete(pending, nextToCommit)
			nextToCommit++
		}
	}()

	var workerWg sync.WaitGroup
	workerWg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer workerWg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}

					if len(job.points) == 0 {
						select {
						case resultCh <- upsertResult{seq: job.seq, nextOffset: job.nextOffset}:
						case <-ctx.Done():
						}
						continue
					}

					targetPoints := make([]*qdrant.PointStruct, len(job.points))
					for i, point := range job.points {
						targetPoints[i] = &qdrant.PointStruct{
							Id:      point.Id,
							Payload: point.Payload,
							Vectors: convertVectorsFromPoint(point),
						}
					}

					_, err := targetClient.Upsert(ctx, &qdrant.UpsertPoints{
						CollectionName: targetCollection,
						Points:         targetPoints,
						Wait:           qdrant.PtrOf(true),
					})
					if err != nil {
						select {
						case resultCh <- upsertResult{seq: job.seq, err: fmt.Errorf("failed to insert data into target: %w", err)}:
						case <-ctx.Done():
						}
						return
					}

					select {
					case resultCh <- upsertResult{seq: job.seq, nextOffset: job.nextOffset, count: len(job.points)}:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	scrollClient := sourceClient.GetPointsClient()
	withPayload := qdrant.NewWithPayload(true)
	withVectors := qdrant.NewWithVectors(true)
	scrollRequest := &qdrant.ScrollPoints{
		CollectionName: sourceCollection,
		Limit:          &limit,
		WithPayload:    withPayload,
		WithVectors:    withVectors,
	}

	seq := 0
	var runErr error

loop:
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				break loop
			}
			if err != nil {
				runErr = err
			}
			break loop
		default:
		}

		scrollRequest.Offset = offsetId

		resp, err := scrollClient.Scroll(ctx, scrollRequest)
		if err != nil {
			runErr = fmt.Errorf("failed to scroll data from source: %w", err)
			sendErr(runErr)
			break
		}

		points := resp.GetResult()
		if len(points) == 0 {
			break
		}

		nextOffset := resp.GetNextPageOffset()

		job := upsertJob{
			seq:        seq,
			points:     points,
			nextOffset: nextOffset,
		}
		seq++

		select {
		case jobs <- job:
		case <-ctx.Done():
			break loop
		}

		offsetId = nextOffset

		if nextOffset == nil {
			break
		}
	}

	close(jobs)
	workerWg.Wait()
	close(resultCh)
	commitWg.Wait()

	var reportedErr error
	for err := range errCh {
		if err != nil {
			reportedErr = err
		}
	}

	if reportedErr != nil {
		return reportedErr
	}

	if runErr != nil {
		return runErr
	}

	pterm.Success.Printfln("Data migration finished successfully")

	return nil
}

func convertVectorOutput(vector *qdrant.VectorOutput) *qdrant.Vector {
	if vector == nil {
		return nil
	}

	return &qdrant.Vector{
		Data:         vector.GetData(),
		Indices:      vector.GetIndices(),
		VectorsCount: vector.VectorsCount,
	}
}

func convertNamedVectorsOutput(vectors map[string]*qdrant.VectorOutput) map[string]*qdrant.Vector {
	if len(vectors) == 0 {
		return nil
	}

	result := make(map[string]*qdrant.Vector, len(vectors))
	for key, value := range vectors {
		result[key] = convertVectorOutput(value)
	}

	return result
}

func convertVectorsOutput(vectors *qdrant.NamedVectorsOutput) *qdrant.NamedVectors {
	if vectors == nil {
		return nil
	}

	return &qdrant.NamedVectors{
		Vectors: convertNamedVectorsOutput(vectors.GetVectors()),
	}
}

func convertVectorsFromPoint(point *qdrant.RetrievedPoint) *qdrant.Vectors {
	if point == nil || point.Vectors == nil {
		return nil
	}

	if vector := point.Vectors.GetVector(); vector != nil {
		return &qdrant.Vectors{
			VectorsOptions: &qdrant.Vectors_Vector{
				Vector: convertVectorOutput(vector),
			},
		}
	}

	if vectors := point.Vectors.GetVectors(); vectors != nil {
		return &qdrant.Vectors{
			VectorsOptions: &qdrant.Vectors_Vectors{
				Vectors: convertVectorsOutput(vectors),
			},
		}
	}

	return nil
}
