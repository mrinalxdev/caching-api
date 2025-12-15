package locking

import (
	"sync"
	"time"
)


type VersionManager struct {
	versions map[string]int
	mutex sync.RWMutex
	ttl map[string]time.Time
}


func NewVersionManager() *VersionManager {
	return &VersionManager{
		versions: make(map[string]int),
		ttl : make(map[string]time.Time),
	}
}


func (vm *VersionManager) GetVersion(key string) (int, bool) {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()
	
	
	if expiry, exists := vm.ttl[key]; exists && time.Now().After(expiry){
		return 0, false
	}
	
	version, exists := vm.versions[key]
	
	return version, exists
}