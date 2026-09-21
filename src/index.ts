import { SyncBackend } from "@livestore/common"
import {
  Effect,
  Stream,
  SubscriptionRef,
} from '@livestore/utils/effect'


type SyncMetadata = {}

export interface WsSyncOptions {
  /**
   * URL of the sync backend
   *
   * The protocol can either `http`/`https` or `ws`/`wss`
   *
   * @example 'https://sync.example.com'
   */
  url: string
}

export const makeRivetSyncBackend = (options: WsSyncOptions): SyncBackend.SyncBackendConstructor<SyncMetadata> => (
	_,
) =>
	Effect.gen(function* () {
		const isConnected = yield* SubscriptionRef.make(false);

		return SyncBackend.of<SyncMetadata>({
			isConnected,
			connect: Effect.void,
			pull: () => Stream.empty,
			push: () => Effect.void,
			metadata: {
				name: "livestore-sync-rivet-actors",
				description: "LiveStore sync backend implementation using Rivet Actors",
				protocol: "ws",
				url: options.url,
			},
			ping: Effect.void,
			supports: {
				pullPageInfoKnown: true,
				pullLive: true,
			},
		});
	});
