package zslot

import (
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/ThunderYurts/Zeus/zconst"
)

func TestDiffSlot(t *testing.T) {
	key1 := "dog"
	key2 := "giraffe"
	hash1 := crc32.ChecksumIEEE([]byte(key1)) % zconst.TotalSlotNum
	hash2 := crc32.ChecksumIEEE([]byte(key2)) % zconst.TotalSlotNum
	fmt.Println(hash1)
	fmt.Println(hash2)

}
