/**
 * Firebase Firestore & Authentication Synchronization for LinkCentral
 */
import { initializeApp } from 'firebase/app';
import {
  getAuth,
  GoogleAuthProvider,
  signInWithPopup,
  signOut,
  onAuthStateChanged,
  type User
} from 'firebase/auth';
import {
  getFirestore,
  doc,
  collection,
  setDoc,
  deleteDoc,
  getDocs,
  getDocFromServer,
  onSnapshot,
  writeBatch,
  serverTimestamp,
  type Unsubscribe
} from 'firebase/firestore';
import firebaseConfig from '../firebase-applet-config.json';

// Initialize Firebase App
const app = initializeApp(firebaseConfig);
export const db = getFirestore(app, firebaseConfig.firestoreDatabaseId);
export const auth = getAuth(app);

// Test connection on boot
async function testConnection() {
  try {
    await getDocFromServer(doc(db, 'test', 'connection'));
  } catch (error) {
    if (error instanceof Error && error.message.includes('the client is offline')) {
      console.error('Please check your Firebase configuration.');
    }
  }
}
testConnection();

export enum OperationType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete',
  LIST = 'list',
  GET = 'get',
  WRITE = 'write',
}

export interface FirestoreErrorInfo {
  error: string;
  operationType: OperationType;
  path: string | null;
  authInfo: {
    userId?: string | null;
    email?: string | null;
    emailVerified?: boolean | null;
    isAnonymous?: boolean | null;
    tenantId?: string | null;
    providerInfo?: {
      providerId?: string | null;
      email?: string | null;
    }[];
  };
}

export function handleFirestoreError(error: unknown, operationType: OperationType, path: string | null): never {
  const errInfo: FirestoreErrorInfo = {
    error: error instanceof Error ? error.message : String(error),
    authInfo: {
      userId: auth.currentUser?.uid,
      email: auth.currentUser?.email,
      emailVerified: auth.currentUser?.emailVerified,
      isAnonymous: auth.currentUser?.isAnonymous,
      tenantId: auth.currentUser?.tenantId,
      providerInfo: auth.currentUser?.providerData?.map(provider => ({
        providerId: provider.providerId,
        email: provider.email,
      })) || []
    },
    operationType,
    path
  };
  console.error('Firestore Error: ', JSON.stringify(errInfo));
  throw new Error(JSON.stringify(errInfo));
}

export interface FlatItem {
  id: string;
  userId: string;
  parentId: string;
  type: 'link' | 'group' | 'image' | 'note';
  name: string;
  url?: string;
  description?: string;
  color?: string;
  isPinned?: boolean;
  order: number;
  imageId?: string;
  createdAt: number;
}

export function flattenTree(items: any[], userId: string, parentId = ''): FlatItem[] {
  const list: FlatItem[] = [];
  if (!Array.isArray(items)) return list;

  items.forEach((item, index) => {
    if (!item || !item.id) return;
    const itemType = item.type === 'group' ? 'group' : item.type === 'image' ? 'image' : item.type === 'note' ? 'note' : 'link';
    const flat: FlatItem = {
      id: String(item.id),
      userId,
      parentId: parentId || '',
      type: itemType,
      name: String(item.name || 'Sin título'),
      order: index,
      createdAt: typeof item.createdAt === 'number' ? item.createdAt : Date.now(),
    };
    if (item.url) flat.url = String(item.url);
    if (item.description) flat.description = String(item.description);
    if (item.color) flat.color = String(item.color);
    if (item.isPinned) flat.isPinned = true;
    if (item.imageId) flat.imageId = String(item.imageId);

    list.push(flat);

    if (item.type === 'group' && Array.isArray(item.items)) {
      list.push(...flattenTree(item.items, userId, item.id));
    }
  });

  return list;
}

export function unflattenTree(flatItems: FlatItem[]): any[] {
  const map = new Map<string, any>();
  const sorted = [...flatItems].sort((a, b) => (a.order ?? 0) - (b.order ?? 0));

  sorted.forEach(f => {
    const node: any = {
      id: f.id,
      type: f.type,
      name: f.name,
      createdAt: f.createdAt || Date.now(),
      color: f.color || null,
      isPinned: !!f.isPinned,
    };
    if (f.url) node.url = f.url;
    if (f.description) node.description = f.description;
    if (f.imageId) node.imageId = f.imageId;
    if (f.type === 'group') node.items = [];
    map.set(f.id, node);
  });

  const roots: any[] = [];
  sorted.forEach(f => {
    const item = map.get(f.id);
    if (!item) return;
    if (f.parentId && map.has(f.parentId)) {
      const parent = map.get(f.parentId);
      if (parent && Array.isArray(parent.items)) {
        parent.items.push(item);
      } else {
        roots.push(item);
      }
    } else {
      roots.push(item);
    }
  });

  return roots;
}

// Global Cloud Sync Service
class LinkCentralCloudService {
  private user: User | null = null;
  private unsubscribeItems: Unsubscribe | null = null;
  private unsubscribePrefs: Unsubscribe | null = null;
  private isSaving = false;
  private onDataChangedCallbacks: Array<(items: any[]) => void> = [];
  private onAuthChangedCallbacks: Array<(user: User | null) => void> = [];
  private onSyncStatusCallbacks: Array<(status: 'synced' | 'syncing' | 'error' | 'offline') => void> = [];

  constructor() {
    onAuthStateChanged(auth, async (currentUser) => {
      this.user = currentUser;
      this.notifyAuthChanged(currentUser);

      if (currentUser) {
        this.notifySyncStatus('syncing');
        await this.ensureUserProfile(currentUser);
        this.setupSubscriptions(currentUser.uid);
      } else {
        this.teardownSubscriptions();
        this.notifySyncStatus('offline');
      }
    });
  }

  private async ensureUserProfile(u: User) {
    const userDocRef = doc(db, 'users', u.uid);
    try {
      await setDoc(userDocRef, {
        userId: u.uid,
        email: u.email || '',
        displayName: u.displayName || '',
        updatedAt: serverTimestamp()
      }, { merge: true });
    } catch (e) {
      console.warn('Could not update user profile document:', e);
    }
  }

  private setupSubscriptions(uid: string) {
    this.teardownSubscriptions();

    const itemsColPath = `users/${uid}/items`;
    const itemsCol = collection(db, 'users', uid, 'items');

    this.unsubscribeItems = onSnapshot(itemsCol, (snapshot) => {
      if (snapshot.metadata.hasPendingWrites) {
        // Local in-flight mutation; skip redundant UI overwrite
        return;
      }
      this.notifySyncStatus('syncing');
      const flatList: FlatItem[] = [];
      snapshot.forEach(docSnap => {
        flatList.push(docSnap.data() as FlatItem);
      });
      const tree = unflattenTree(flatList);
      this.notifyDataChanged(tree);
      this.notifySyncStatus('synced');
    }, (error) => {
      this.notifySyncStatus('error');
      handleFirestoreError(error, OperationType.GET, itemsColPath);
    });
  }

  private teardownSubscriptions() {
    if (this.unsubscribeItems) {
      this.unsubscribeItems();
      this.unsubscribeItems = null;
    }
    if (this.unsubscribePrefs) {
      this.unsubscribePrefs();
      this.unsubscribePrefs = null;
    }
  }

  public async signIn() {
    try {
      this.notifySyncStatus('syncing');
      const provider = new GoogleAuthProvider();
      provider.setCustomParameters({ prompt: 'select_account' });
      const result = await signInWithPopup(auth, provider);
      return result.user;
    } catch (error) {
      this.notifySyncStatus('error');
      console.error('Sign-in error:', error);
      throw error;
    }
  }

  public async signOut() {
    try {
      await signOut(auth);
    } catch (error) {
      console.error('Sign-out error:', error);
      throw error;
    }
  }

  public getUser(): User | null {
    return this.user;
  }

  public isSignedIn(): boolean {
    return !!this.user;
  }

  public async saveTree(tree: any[]): Promise<boolean> {
    if (!this.user) return false;
    if (this.isSaving) return false;

    this.isSaving = true;
    this.notifySyncStatus('syncing');
    const uid = this.user.uid;
    const itemsColPath = `users/${uid}/items`;

    try {
      const flatNewItems = flattenTree(tree, uid);
      const newItemsMap = new Map(flatNewItems.map(item => [item.id, item]));

      // Fetch current remote items to determine deletions
      const existingSnap = await getDocs(collection(db, 'users', uid, 'items'));
      const batch = writeBatch(db);

      // Delete removed items
      existingSnap.forEach(snap => {
        if (!newItemsMap.has(snap.id)) {
          batch.delete(doc(db, 'users', uid, 'items', snap.id));
        }
      });

      // Upsert current items
      flatNewItems.forEach(item => {
        const itemRef = doc(db, 'users', uid, 'items', item.id);
        batch.set(itemRef, {
          ...item,
          updatedAt: serverTimestamp()
        });
      });

      await batch.commit();
      this.notifySyncStatus('synced');
      return true;
    } catch (error) {
      this.notifySyncStatus('error');
      handleFirestoreError(error, OperationType.WRITE, itemsColPath);
      return false;
    } finally {
      this.isSaving = false;
    }
  }

  public async savePreferences(prefs: { theme?: string; headerButtonsOrder?: string; customColors?: string }): Promise<void> {
    if (!this.user) return;
    const prefPath = `users/${this.user.uid}/settings/preferences`;
    try {
      await setDoc(doc(db, 'users', this.user.uid, 'settings', 'preferences'), {
        userId: this.user.uid,
        ...prefs,
        updatedAt: serverTimestamp()
      }, { merge: true });
    } catch (e) {
      handleFirestoreError(e, OperationType.WRITE, prefPath);
    }
  }

  public onDataChanged(cb: (items: any[]) => void) {
    this.onDataChangedCallbacks.push(cb);
  }

  public onAuthChanged(cb: (user: User | null) => void) {
    this.onAuthChangedCallbacks.push(cb);
    if (this.user !== undefined) cb(this.user);
  }

  public onSyncStatus(cb: (status: 'synced' | 'syncing' | 'error' | 'offline') => void) {
    this.onSyncStatusCallbacks.push(cb);
    cb(this.user ? 'synced' : 'offline');
  }

  private notifyDataChanged(items: any[]) {
    this.onDataChangedCallbacks.forEach(cb => {
      try { cb(items); } catch (e) { console.error('Error in onDataChanged callback:', e); }
    });
  }

  private notifyAuthChanged(user: User | null) {
    this.onAuthChangedCallbacks.forEach(cb => {
      try { cb(user); } catch (e) { console.error('Error in onAuthChanged callback:', e); }
    });
  }

  private notifySyncStatus(status: 'synced' | 'syncing' | 'error' | 'offline') {
    this.onSyncStatusCallbacks.forEach(cb => {
      try { cb(status); } catch (e) { console.error('Error in onSyncStatus callback:', e); }
    });
  }
}

// Attach instance to window for seamless script interaction
const cloudService = new LinkCentralCloudService();
(window as any).LinkCentralCloud = cloudService;

export default cloudService;
