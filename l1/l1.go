package l1

import (
	"context"
	"fmt"

	"github.com/NethermindEth/juno/blockchain"
	"github.com/NethermindEth/juno/core"
	"github.com/NethermindEth/juno/core/felt"
	"github.com/NethermindEth/juno/l1/internal/contract"
	"github.com/NethermindEth/juno/service"
	"github.com/NethermindEth/juno/utils"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

//go:generate mockgen -destination=./mocks/mock_subscriber.go -package=mocks github.com/NethermindEth/juno/l1 Subscriber
type Subscriber interface {
	WatchHeader(ctx context.Context, sink chan<- *types.Header) (event.Subscription, error)
	WatchLogStateUpdate(ctx context.Context, sink chan<- *contract.StarknetLogStateUpdate) (event.Subscription, error)
}

type Client struct {
	l1                Subscriber
	l2Chain           *blockchain.Blockchain
	log               utils.SimpleLogger
	confirmationQueue *queue
}

var _ service.Service = (*Client)(nil)

func NewClient(l1 Subscriber, chain *blockchain.Blockchain, confirmationPeriod uint64, log utils.SimpleLogger) *Client {
	return &Client{
		l1:                l1,
		l2Chain:           chain,
		log:               log,
		confirmationQueue: newQueue(confirmationPeriod),
	}
}

func (c *Client) subscribeToHeaders(ctx context.Context, buffer int) (event.Subscription, chan *types.Header, error) {
	headerChan := make(chan *types.Header, buffer)
	sub, err := c.l1.WatchHeader(ctx, headerChan)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe to L1 headers: %w", err)
	}
	return sub, headerChan, nil
}

func (c *Client) subscribeToUpdates(ctx context.Context, buffer int) (event.Subscription, chan *contract.StarknetLogStateUpdate, error) {
	logStateUpdateChan := make(chan *contract.StarknetLogStateUpdate, buffer)
	sub, err := c.l1.WatchLogStateUpdate(ctx, logStateUpdateChan)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe to L1 state updates: %w", err)
	}
	return sub, logStateUpdateChan, nil
}

func (c *Client) Run(ctx context.Context) error {
	buffer := 128
	subscribeCtx, subscribeCtxCancel := context.WithCancel(ctx)
	defer subscribeCtxCancel()

	subUpdates, logStateUpdateChan, err := c.subscribeToUpdates(subscribeCtx, buffer)
	if err != nil {
		return err
	}
	defer subUpdates.Unsubscribe()

	subHeaders, headerChan, err := c.subscribeToHeaders(subscribeCtx, buffer)
	if err != nil {
		return err
	}
	defer subHeaders.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-subHeaders.Err():
			c.log.Warnw("L1 header subscription failed, resubscribing", "error", err)
			subHeaders.Unsubscribe()

			subHeaders, headerChan, err = c.subscribeToHeaders(subscribeCtx, buffer)
			if err != nil {
				return err
			}
			defer subHeaders.Unsubscribe() //nolint:gocritic
		case header := <-headerChan:
			l1Height := header.Number.Uint64()
			c.log.Debugw("Received L1 header", "number", l1Height, "hash", header.Hash().Hex())
		Outer:
			// Check for updates in this block.
			// We need to loop in case there were multiple LogStateUpdates emitted.
			for {
				select {
				case err := <-subUpdates.Err():
					c.log.Warnw("L1 update subscription failed, resubscribing", "error", err)
					subUpdates.Unsubscribe()

					subUpdates, logStateUpdateChan, err = c.subscribeToUpdates(subscribeCtx, buffer)
					if err != nil {
						return err
					}
					defer subUpdates.Unsubscribe() //nolint:gocritic
				case logStateUpdate := <-logStateUpdateChan:
					c.log.Debugw("Received L1 LogStateUpdate",
						"number", logStateUpdate.BlockNumber,
						"stateRoot", logStateUpdate.GlobalRoot.Text(felt.Base16),
						"blockHash", logStateUpdate.BlockHash.Text(felt.Base16))
					if logStateUpdate.Raw.Removed {
						// NOTE: we only modify the local confirmationQueue upon receiving reorged logs.
						// We assume new logs will soon follow, so we don't notify the l2Chain.
						c.confirmationQueue.Reorg(logStateUpdate.Raw.BlockNumber)
					} else {
						c.confirmationQueue.Enqueue(logStateUpdate)
					}
				default:
					break Outer
				}
			}

			// Set the chain head to the max confirmed log, if it exists.
			if maxConfirmed := c.confirmationQueue.MaxConfirmed(l1Height); maxConfirmed != nil {
				head := &core.L1Head{
					BlockNumber: maxConfirmed.BlockNumber.Uint64(),
					BlockHash:   new(felt.Felt).SetBigInt(maxConfirmed.BlockHash),
					StateRoot:   new(felt.Felt).SetBigInt(maxConfirmed.GlobalRoot),
				}
				if err := c.l2Chain.SetL1Head(head); err != nil {
					return fmt.Errorf("l1 head for block %d and state root %s: %w", head.BlockNumber, head.StateRoot.String(), err)
				}
				c.confirmationQueue.Remove(maxConfirmed.Raw.BlockNumber)
				c.log.Infow("Updated l1 head",
					"blockNumber", head.BlockNumber,
					"blockHash", head.BlockHash.ShortString(),
					"stateRoot", head.StateRoot.ShortString())
			}
		}
	}
}
