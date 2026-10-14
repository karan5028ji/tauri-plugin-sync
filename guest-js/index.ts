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

/**
 * Safe frontend configuration tuning parameters.
 * Sensitive parameters (credentials, database path, remote endpoints)
 * are locked in Rust / tauri.conf.json to protect against WebView XSS vulnerabilities.
 */
export interface FrontendSyncConfig {
  syncIntervalMs?: number;
  batchSize?: number;
  pruneDays?: number;
}

/**
 * Triggers an immediate push/pull sync pass.
 */
export async function syncNow(): Promise<SyncProgressEvent> {
  return await invoke<SyncProgressEvent>('plugin:sync|sync_now');
}

/**
 * Returns current sync status (state, pending changes count, last sync timestamp).
 */
export async function getStatus(): Promise<SyncStatus> {
  return await invoke<SyncStatus>('plugin:sync|get_status');
}

/**
 * Pauses the background synchronization polling loop.
 */
export async function pause(): Promise<void> {
  await invoke<void>('plugin:sync|pause');
}

/**
 * Resumes the background synchronization polling loop.
 */
export async function resume(): Promise<void> {
  await invoke<void>('plugin:sync|resume');
}

/**
 * Updates safe frontend configuration options (e.g. sync interval, batch size).
 */
export async function configure(config: FrontendSyncConfig): Promise<void> {
  await invoke<void>('plugin:sync|configure', {
    config: {
      sync_interval_ms: config.syncIntervalMs ?? null,
      batch_size: config.batchSize ?? null,
      prune_days: config.pruneDays ?? null,
    },
  });
}

/**
 * Subscribes to sync state changes ('idle', 'syncing', 'offline', 'error').
 */
export async function onStatusChange(callback: (status: SyncStatus) => void): Promise<UnlistenFn> {
  return await listen<SyncStatus>('sync://status', (event) => {
    callback(event.payload);
  });
}

/**
 * Subscribes to sync progress events (number of pushed and pulled changesets).
 */
export async function onProgress(callback: (progress: SyncProgressEvent) => void): Promise<UnlistenFn> {
  return await listen<SyncProgressEvent>('sync://progress', (event) => {
    callback(event.payload);
  });
}

/**
 * Subscribes to 401 Unauthorized events from the sync engine.
 */
export async function onUnauthorized(callback: () => void): Promise<UnlistenFn> {
  return await listen('sync://unauthorized', () => {
    callback();
  });
}

/**
 * SyncManager provides the primary object-oriented interface to tauri-plugin-sync.
 * Zero configuration is required on the frontend when configured via the Rust Builder.
 */
export class SyncManager {
  private statusUnlisten: UnlistenFn | null = null;
  private progressUnlisten: UnlistenFn | null = null;
  private unauthorizedUnlisten: UnlistenFn | null = null;

  /**
   * Initializes or connects to the sync engine instance.
   * Optionally updates safe frontend configurations.
   */
  public static async init(config?: FrontendSyncConfig): Promise<SyncManager> {
    const manager = new SyncManager();
    if (config) {
      await manager.configure(config);
    }
    return manager;
  }

  public async syncNow(): Promise<SyncProgressEvent> {
    return await syncNow();
  }

  public async getStatus(): Promise<SyncStatus> {
    return await getStatus();
  }

  public async pause(): Promise<void> {
    await pause();
  }

  public async resume(): Promise<void> {
    await resume();
  }

  public async configure(config: FrontendSyncConfig): Promise<void> {
    await configure(config);
  }

  public async onStatusChange(callback: (status: SyncStatus) => void): Promise<UnlistenFn> {
    const unlisten = await onStatusChange(callback);
    this.statusUnlisten = unlisten;
    return unlisten;
  }

  public async onProgress(callback: (progress: SyncProgressEvent) => void): Promise<UnlistenFn> {
    const unlisten = await onProgress(callback);
    this.progressUnlisten = unlisten;
    return unlisten;
  }

  public async onUnauthorized(callback: () => void): Promise<UnlistenFn> {
    const unlisten = await onUnauthorized(callback);
    this.unauthorizedUnlisten = unlisten;
    return unlisten;
  }

  public destroy(): void {
    if (this.statusUnlisten) {
      this.statusUnlisten();
      this.statusUnlisten = null;
    }
    if (this.progressUnlisten) {
      this.progressUnlisten();
      this.progressUnlisten = null;
    }
    if (this.unauthorizedUnlisten) {
      this.unauthorizedUnlisten();
      this.unauthorizedUnlisten = null;
    }
  }
}
