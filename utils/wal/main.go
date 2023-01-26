package main

import (
	"strconv"
)

func main() {
	// f, err := os.OpenFile("file.txt", os.O_RDWR|os.O_CREATE, 0777)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer os.Remove("file.txt")
	// defer f.Close()
	// // file length must be > 0
	// f.Truncate(1)

	// // Map maps an entire file into memory
	// // prot argument
	// // mmap.RDONLY - Maps the memory read-only. Attempts to write to the MMap object will result in undefined behavior.
	// // mmap.RDWR - Maps the memory as read-write. Writes to the MMap object will update the underlying file.
	// // mmap.COPY - Writes to the MMap object will affect memory, but the underlying file will remain unchanged.
	// // mmap.EXEC - The mapped memory is marked as executable.
	// // flag argument
	// // mmap.ANON - The mapped memory will not be backed by a file. If ANON is set in flags, f is ignored.
	// mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
	// defer mmapFile.Unmap()

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Printf("File length: %d\n", len(mmapFile))
	// fmt.Printf("File contents: %s\n", string(mmapFile))
	// mmapFile[0] = []byte("a")[0]
	// fmt.Printf("File contents after writing one byte of data: %s\n", string(mmapFile))

	// err = f.Truncate(2)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("File length after truncation: %d\n", len(mmapFile))
	// fmt.Printf("File contents after truncation: %s\n", string(mmapFile))
	// // we must map the file again
	// mmapFile, err = mmap.Map(f, mmap.RDWR, 0)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("File length after remapping: %d\n", len(mmapFile))
	// fmt.Printf("File contents after remapping: %s\n", string(mmapFile))
	// // copying elements to dst slice from src slice
	// copy(mmapFile, []byte("hi"))
	// fmt.Printf("File contents after copying data from a byte slice: %s\n", string(mmapFile))

	w := NewWAL("nosql/", uint32(20), uint32(4))
	for i := 1; i <= 81; i++ {
		if i%6 != 0 {
			w.putEntry("kljuc"+strconv.Itoa(i), []byte{1, 2, 3, 4}, byte(0))
		} else {
			w.putEntry("kljuc"+strconv.Itoa(i), []byte{2, 2, 5, 6}, byte(1))
		}
	}
	//w.emptyWAL()
	//w.removeOldSegments()
}
