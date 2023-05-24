package sync

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/NethermindEth/juno/blockchain"
	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/db"
	"github.com/NethermindEth/juno/service"
	"github.com/NethermindEth/juno/starknetdata"
	"github.com/NethermindEth/juno/utils"
	"github.com/sourcegraph/conc/stream"
)

var (
	_ service.Service = (*Synchronizer)(nil)

	defaultPendingPollInterval = 5 * time.Second
)

// Synchronizer manages a list of StarknetData to fetch the latest blockchain updates
type Synchronizer struct {
	Blockchain          *blockchain.Blockchain
	StarknetData        starknetdata.StarknetData
	StartingBlockNumber *uint64
	HighestBlockHeader  *core.Header

	log utils.SimpleLogger
}

func New(bc *blockchain.Blockchain, starkNetData starknetdata.StarknetData, log utils.SimpleLogger) *Synchronizer {
	return &Synchronizer{
		Blockchain:   bc,
		StarknetData: starkNetData,
		log:          log,
	}
}

// Run starts the Synchronizer, returns an error if the loop is already running
func (s *Synchronizer) Run(ctx context.Context) error {
	s.syncBlocks(ctx)
	return nil
}

func (s *Synchronizer) fetcherTask(ctx context.Context, height uint64, verifiers *stream.Stream,
	resetStreams context.CancelFunc,
) stream.Callback {
	for {
		select {
		case <-ctx.Done():
			return func() {}
		default:
			block, err := s.StarknetData.BlockByNumber(ctx, height)
			if err != nil {
				continue
			}
			stateUpdate, err := s.StarknetData.StateUpdate(ctx, height)
			if err != nil {
				continue
			}

			newClasses, err := s.fetchUnknownClasses(ctx, stateUpdate)
			if err != nil {
				continue
			}

			return func() {
				verifiers.Go(func() stream.Callback {
					return s.verifierTask(ctx, block, stateUpdate, newClasses, resetStreams)
				})
			}
		}
	}
}

func (s *Synchronizer) fetchUnknownClasses(ctx context.Context, stateUpdate *core.StateUpdate) (map[felt.Felt]core.Class, error) {
	state, closer, err := s.Blockchain.HeadState()
	if err != nil {
		// if err is db.ErrKeyNotFound we are on an empty DB
		if !errors.Is(err, db.ErrKeyNotFound) {
			return nil, err
		}
		closer = func() error {
			return nil
		}
	}

	newClasses := make(map[felt.Felt]core.Class)
	fetchIfNotFound := func(classHash *felt.Felt) error {
		if _, ok := newClasses[*classHash]; ok {
			return nil
		}

		stateErr := db.ErrKeyNotFound
		if state != nil {
			_, stateErr = state.Class(classHash)
		}

		if errors.Is(stateErr, db.ErrKeyNotFound) {
			class, fetchErr := s.StarknetData.Class(ctx, classHash)
			if fetchErr == nil {
				newClasses[*classHash] = class
			}
			return fetchErr
		}
		return stateErr
	}

	for _, deployedContract := range stateUpdate.StateDiff.DeployedContracts {
		if err = fetchIfNotFound(deployedContract.ClassHash); err != nil {
			return nil, db.CloseAndWrapOnError(closer, err)
		}
	}
	for _, classHash := range stateUpdate.StateDiff.DeclaredV0Classes {
		if err = fetchIfNotFound(classHash); err != nil {
			return nil, db.CloseAndWrapOnError(closer, err)
		}
	}
	for _, declaredV1 := range stateUpdate.StateDiff.DeclaredV1Classes {
		if err = fetchIfNotFound(declaredV1.ClassHash); err != nil {
			return nil, db.CloseAndWrapOnError(closer, err)
		}
	}

	return newClasses, db.CloseAndWrapOnError(closer, nil)
}

func (s *Synchronizer) verifierTask(ctx context.Context, block *core.Block, stateUpdate *core.StateUpdate,
	newClasses map[felt.Felt]core.Class, resetStreams context.CancelFunc,
) stream.Callback {
	err := s.Blockchain.SanityCheckNewHeight(block, stateUpdate, newClasses)
	return func() {
		select {
		case <-ctx.Done():
			return
		default:
			if err != nil {
				s.log.Warnw("Sanity checks failed", "number", block.Number, "hash", block.Hash.ShortString())
				resetStreams()
				return
			}
			err = s.Blockchain.Store(block, stateUpdate, newClasses)
			if err != nil {
				s.log.Warnw("Failed storing Block", "number", block.Number,
					"hash", block.Hash.ShortString(), "err", err.Error())
				resetStreams()
				return
			}

			if s.HighestBlockHeader == nil || s.HighestBlockHeader.Number < block.Number {
				highestBlock, err := s.StarknetData.BlockLatest(ctx)
				if err != nil {
					s.log.Warnw("Failed fetching latest block", "err", err.Error())
				} else {
					s.HighestBlockHeader = highestBlock.Header
				}
			}

			s.log.Infow("Stored Block", "number", block.Number, "hash",
				block.Hash.ShortString(), "root", block.GlobalStateRoot.ShortString())
		}
	}
}

func (s *Synchronizer) nextHeight() uint64 {
	nextHeight := uint64(0)
	if h, err := s.Blockchain.Height(); err == nil {
		nextHeight = h + 1
	}
	return nextHeight
}

func (s *Synchronizer) syncBlocks(syncCtx context.Context) {
	defer func() {
		s.StartingBlockNumber = nil
		s.HighestBlockHeader = nil
	}()

	fetchers := stream.New().WithMaxGoroutines(runtime.NumCPU())
	fetchersSem := make(chan struct{}, runtime.NumCPU())
	verifiers := stream.New().WithMaxGoroutines(runtime.NumCPU())

	pendingPollTicker := time.NewTicker(defaultPendingPollInterval)
	pendingPollSem := make(chan struct{}, 1)

	streamCtx, streamCancel := context.WithCancel(syncCtx)

	nextHeight := s.nextHeight()
	startingHeight := nextHeight
	s.StartingBlockNumber = &startingHeight

	for {
		select {
		case <-streamCtx.Done():
			select {
			case <-syncCtx.Done():
				streamCancel()
				fetchers.Wait()
				verifiers.Wait()
				pendingPollTicker.Stop()
				pendingPollSem <- struct{}{}
				return
			default:
				streamCtx, streamCancel = context.WithCancel(syncCtx)
				nextHeight = s.nextHeight()
				s.log.Warnw("Rolling back sync process", "height", nextHeight)
			}
		case <-pendingPollTicker.C:
			select {
			case pendingPollSem <- struct{}{}:
				go func() {
					if err := s.pollPending(syncCtx); err != nil {
						s.log.Debugw("Error while trying to poll pending block", "err", err)
					}
					<-pendingPollSem
				}()
			default:
				// poll already in-progress, ignore the tick
			}
		case fetchersSem <- struct{}{}:
			curHeight, curStreamCtx, curCancel := nextHeight, streamCtx, streamCancel
			fetchers.Go(func() stream.Callback {
				cb := s.fetcherTask(curStreamCtx, curHeight, verifiers, curCancel)
				<-fetchersSem
				return cb
			})
			nextHeight++
		}
	}
}

func (s *Synchronizer) pollPending(ctx context.Context) error {
	if s.HighestBlockHeader != nil {
		head, err := s.Blockchain.HeadsHeader()
		if err != nil {
			return err
		}

		// not at the tip of the chain yet, no need to poll pending
		if s.HighestBlockHeader.Number > head.Number {
			return nil
		}
	}

	pendingBlock, err := s.StarknetData.BlockPending(ctx)
	if err != nil {
		return err
	}

	pendingStateUpdate, err := s.StarknetData.StateUpdatePending(ctx)
	if err != nil {
		return err
	}

	newClasses, err := s.fetchUnknownClasses(ctx, pendingStateUpdate)
	if err != nil {
		return err
	}

	s.log.Debugw("Found pending block", "txns", pendingBlock.TransactionCount)
	return s.Blockchain.StorePending(&blockchain.Pending{
		Block:       pendingBlock,
		StateUpdate: pendingStateUpdate,
		NewClasses:  newClasses,
	})
}
