// Verify Raw chunk v1 framing and CRC32C without Python or third-party packages.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
)

type vector struct {
	ChunkBytes int    `json:"chunk_bytes"`
	ChunkHex   string `json:"chunk_hex"`
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}

func main() {
	body, err := os.ReadFile("tests/golden/raw_chunk_v1.json")
	if err != nil {
		fail(err.Error())
	}
	var expected vector
	if err = json.Unmarshal(body, &expected); err != nil {
		fail(err.Error())
	}
	chunk, err := hex.DecodeString(expected.ChunkHex)
	if err != nil || len(chunk) != expected.ChunkBytes {
		fail("invalid golden chunk hex or byte count")
	}
	if string(chunk[:8]) != "BMRCHNK\x1a" || chunk[8] != 1 || chunk[9] != 0 {
		fail("invalid magic or version")
	}
	if binary.BigEndian.Uint16(chunk[10:12]) != 0xfeff {
		fail("invalid byte-order marker")
	}
	headerBodyLength := int(binary.BigEndian.Uint32(chunk[16:20]))
	headerEnd := 24 + headerBodyLength
	table := crc32.MakeTable(crc32.Castagnoli)
	headerCovered := append(append([]byte{}, chunk[:20]...), chunk[24:headerEnd]...)
	if crc32.Checksum(headerCovered, table) != binary.BigEndian.Uint32(chunk[20:24]) {
		fail("header CRC32C mismatch")
	}
	frame := chunk[headerEnd:]
	bodyLength := int(binary.BigEndian.Uint32(frame[:4]))
	if len(frame) != 12+bodyLength {
		fail("frame length mismatch")
	}
	frameCovered := append(append([]byte{}, frame[:8]...), frame[12:]...)
	if crc32.Checksum(frameCovered, table) != binary.BigEndian.Uint32(frame[8:12]) {
		fail("frame CRC32C mismatch")
	}
	fmt.Println("Go raw-chunk.v1 golden verification passed")
}
