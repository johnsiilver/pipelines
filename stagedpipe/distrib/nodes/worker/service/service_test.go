package service

import (
	"io/fs"
	"time"
)

// mockFileInfo creates a mock fs.FileInfo
type mockFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (mfi mockFileInfo) Name() string       { return mfi.name }
func (mfi mockFileInfo) Size() int64        { return mfi.size }
func (mfi mockFileInfo) Mode() fs.FileMode  { return 0 }
func (mfi mockFileInfo) ModTime() time.Time { return mfi.modTime }
func (mfi mockFileInfo) IsDir() bool        { return mfi.isDir }
func (mfi mockFileInfo) Sys() interface{}   { return nil }

/*
func TestLoadPlugin(t *testing.T) {
	// Mock server instance
	s := &Server{} // Replace with actual Server constructor if needed

	// Mock context
	ctx := context.Background()

	// Mock file info
	fi := mockFileInfo{
		name:    "testPlugin",
		size:    1024,
		modTime: time.Now(),
		isDir:   false,
	}

	// Mock file path
	fqPath := filepath.Join("path", "to", "plugin", "testPlugin")

	// Create a temporary file to simulate the plugin
	_, err := os.Create(fqPath)
	if err != nil {
		t.Fatalf("Failed to create mock plugin file: %s", err)
	}
	defer os.Remove(fqPath) // Clean up

	// Invoke the loadPlugin method
	err = s.loadPlugin(ctx, fqPath, fi)

	// Assert the expected results
	// Adjust these assertions based on the expected behavior of loadPlugin
	if err != nil {
		t.Errorf("loadPlugin returned an error: %s", err)
	}

	// Additional assertions can be added here based on the specific behavior of loadPlugin
}
*/
