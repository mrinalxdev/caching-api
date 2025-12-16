package locking

import (
	"sync"
	"time"
)

type VersionManager struct {
	versions map[string]int
	mutex    sync.RWMutex
	ttl      map[string]time.Time
}

func NewVersionManager() *VersionManager {
	return &VersionManager{
		versions: make(map[string]int),
		ttl:      make(map[string]time.Time),
	}
}

func (vm *VersionManager) GetVersion(key string) (int, bool) {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()
	
	if expiry, exists := vm.ttl[key]; exists && time.Now().After(expiry) {
		return 0, false
	}
	
	version, exists := vm.versions[key]
	return version, exists
}

func (vm *VersionManager) SetVersion(key string, version int) {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()
	
	vm.versions[key] = version
	vm.ttl[key] = time.Now().Add(5 * time.Minute)
}

func (vm *VersionManager) IncrementVersion(key string) int {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()
	
	current := vm.versions[key]
	vm.versions[key] = current + 1
	vm.ttl[key] = time.Now().Add(5 * time.Minute)
	
	return vm.versions[key]
}

func (vm *VersionManager) CheckAndSet(key string, expected, newVersion int) bool {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()
	
	current, exists := vm.versions[key]
	if !exists || current != expected {
		return false
	}
	
	vm.versions[key] = newVersion
	vm.ttl[key] = time.Now().Add(5 * time.Minute)
	return true
}

func (vm *VersionManager) CleanupExpired() {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()
	
	now := time.Now()
	for key, expiry := range vm.ttl {
		if now.After(expiry) {
			delete(vm.versions, key)
			delete(vm.ttl, key)
		}
	}
}