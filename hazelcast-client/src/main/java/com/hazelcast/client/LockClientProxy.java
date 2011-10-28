/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.client;

import static com.hazelcast.client.ProxyHelper.check;

import com.hazelcast.core.ILock;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.monitor.LocalLockStats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

public class LockClientProxy implements ILock {
    final ProxyHelper proxyHelper;
    final Object lockObject;
//    final HazelcastClient client;

    public LockClientProxy(Object object, HazelcastClient client) {
        proxyHelper = new ProxyHelper("", client);
        lockObject = object;
//        this.client = client;
        check(lockObject);
    }

    public Object getLockObject() {
        return lockObject;
    }

//    public void lock() {
//        client.mapLockProxy.lock(lockObject);
//    }
//
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException("lockInterruptibly is not implemented!");
    }
//
//    public boolean tryLock() {
//        return client.mapLockProxy.tryLock(lockObject);
//    }
//
//    public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
//        ProxyHelper.checkTime(l, timeUnit);
//        return client.mapLockProxy.tryLock(lockObject, l, timeUnit);
//    }
//
//    public void unlock() {
//        client.mapLockProxy.unlock(lockObject);
//    }
    
    public void lock() {
        doLock(-1, null);
    }

    public boolean tryLock() {
        return (Boolean) doLock(0, null);
    }
    
    public boolean tryLock(long time, TimeUnit timeunit) {
        ProxyHelper.checkTime(time, timeunit);
        return (Boolean) doLock(time, timeunit);
    }

    public void unlock() {
        proxyHelper.doOp(ClusterOperation.LOCK_UNLOCK, lockObject, null);
    }
    
    private Object doLock(long timeout, TimeUnit timeUnit) {
    	ClusterOperation operation = ClusterOperation.LOCK_LOCK;
        Packet request = proxyHelper.prepareRequest(operation, lockObject, null);
        request.setTimeout(timeUnit == null ? timeout : timeUnit.toMillis(timeout));
        Packet response = proxyHelper.callAndGetResult(request);
        return proxyHelper.getValue(response);
    }

    public Condition newCondition() {
        return null;
    }

    public InstanceType getInstanceType() {
        return InstanceType.LOCK;
    }

    public void destroy() {
        proxyHelper.destroy();
    }

    public Object getId() {
        return new FactoryImpl.ProxyKey("lock", lockObject);
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof ILock) {
            return getId().equals(((ILock) o).getId());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

	public LocalLockStats getLocalLockStats() {
		throw new UnsupportedOperationException();
	}
}
