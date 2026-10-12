import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';

export type SyncState = 'idle' | 'syncing' | 'offline' | 'error';

export interface SyncStatus {
  state: SyncState;
  last_sync_at: number | null;
  pending_changes: number;
  last_error: string | null;
}

export interface SyncProgressEvent {
  pushed_count: number;
  pulled_count: number;
  watermark: number;
}

export interface SyncConfig {
  database: string;
  endpoint: string;
  tables: string[];
  authHeader?: string;
  getAuthHeader?: () => Promise<string> | string;
  onTokenExpired?: () => Promise<string>;
  syncIntervalMs?: number;
  batchSize?: number;
  pruneDays?: number;
}

/**
 * SyncManager provides the primary interface to tauri-plugin-sync.
 */
export class SyncManager {
  private config: SyncConfig;
  private unauthorizedUnlisten: UnlistenFn | null = null;
  private isRefreshingToken = false;

  private constructor(config: SyncConfig) {
    this.config = config;
  }

  /**
   * Initializes SQLite shadow tables, triggers, and the background sync worker.
   */
  public static async init(config: SyncConfig): Promise<SyncManager> {
    let initialAuthHeader = config.authHeader;
    if (!initialAuthHeader && config.getAuthHeader) {
      initialAuthHeader = await config.getAuthHeader();
    }

    await invoke<SyncStatus>('plugin:sync|init', {
      config: {
        database: config.database,
        endpoint: config.endpoint,
        tables: config.tables,
        auth_header: initialAuthHeader ?? null,
        sync_interval_ms: config.syncIntervalMs ?? 5000,
        batch_size: config.batchSize ?? 500,
        prune_days: config.pruneDays ?? 30,
      },
    });

    const manager = new SyncManager(config);
    await manager.setupUnauthorizedListener();
    return manager;
  }

  private async setupUnauthorizedListener(): Promise<void> {
    this.unauthorizedUnlisten = await listen('sync://unauthorized', async () => {
      if (this.isRefreshingToken) return;

      if (this.config.onTokenExpired) {
        try {
          this.isRefreshingToken = true;
          const newToken = await this.config.onTokenExpired();
          this.config.authHeader = newToken;
          await this.configure({ ...this.config, authHeader: newToken });
          // Automatically trigger an immediate retry pass with the refreshed token
          await this.syncNow();
        } catch (err) {
          console.error('[tauri-plugin-sync] Token refresh callback failed:', err);
        } finally {
          this.isRefreshingToken = false;
        }
      }
    });
  }

  /**
   * Triggers an immediate push/pull sync pass.
   */
  public async syncNow(): Promise<SyncProgressEvent> {
    return await invoke<SyncProgressEvent>('plugin:sync|sync_now');
  }

  /**
   * Returns current sync status (state, pending changes count, last sync timestamp).
   */
  public async getStatus(): Promise<SyncStatus> {
    return await invoke<SyncStatus>('plugin:sync|get_status');
  }

  /**
   * Updates configuration dynamically (endpoint, auth token, tables, sync interval).
   */
  public async configure(config: SyncConfig): Promise<void> {
    this.config = config;
    await invoke<void>('plugin:sync|configure', {
      config: {
        database: config.database,
        endpoint: config.endpoint,
        tables: config.tables,
        auth_header: config.authHeader ?? null,
        sync_interval_ms: config.syncIntervalMs ?? 5000,
        batch_size: config.batchSize ?? 500,
        prune_days: config.pruneDays ?? 30,
      },
    });
  }

  /**
   * Subscribes to sync state changes ('idle', 'syncing', 'offline', 'error').
   */
  public async onStatusChange(callback: (status: SyncStatus) => void): Promise<UnlistenFn> {
    return await listen<SyncStatus>('sync://status', (event) => {
      callback(event.payload);
    });
  }

  /**
   * Subscribes to sync progress events (number of pushed and pulled changesets).
   */
  public async onProgress(callback: (progress: SyncProgressEvent) => void): Promise<UnlistenFn> {
    return await listen<SyncProgressEvent>('sync://progress', (event) => {
      callback(event.payload);
    });
  }

  /**
   * Subscribes to 401 Unauthorized events.
   */
  public async onUnauthorized(callback: () => void): Promise<UnlistenFn> {
    return await listen('sync://unauthorized', () => {
      callback();
    });
  }

  /**
   * Cleans up event listeners.
   */
  public destroy(): void {
    if (this.unauthorizedUnlisten) {
      this.unauthorizedUnlisten();
      this.unauthorizedUnlisten = null;
    }
  }
}
