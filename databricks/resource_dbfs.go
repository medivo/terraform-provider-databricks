package databricks

import (
	"context"
	"io"
	"os"
	"path"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
	db "github.com/medivo/databricks-go"
)

func resourceDBFS() *schema.Resource {
	return &schema.Resource{
		Create: resourceDBFSCreate,
		Read:   resourceDBFSRead,
		Update: resourceDBFSUpdate,
		Delete: resourceDBFSDelete,
		Schema: map[string]*schema.Schema{
			"dbfs_path": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"source": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},
			"is_directory": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
			"file_size": &schema.Schema{
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}

func resourceDBFSCreate(data *schema.ResourceData, client interface{}) error {
	// steps
	// 1) Issue a create call and get a handle.
	// 2) Issue one or more add-block (1mb max) calls with the handle you have.
	// 3) Issue a close call with the handle you have.

	ctx := context.Background()
	dbfsPath := data.Get("dbfs_path").(string)
	src, ok := data.Get("source").(string)

	// if there is a source then it's not a directory...
	if !ok || len(src) == 0 {
		err := client.(*db.Client).DBFS().Mkdirs(
			ctx,
			dbfsPath,
		)
		if err != nil {
			return err
		}
		data.Set("is_directory", true)
		data.SetId(dbfsPath)
		return nil
	}

	// do a mkdir -p :)
	client.(*db.Client).DBFS().Mkdirs(
		ctx,
		path.Dir(dbfsPath),
	)

	return dbfsUploadFile(
		context.Background(),
		client,
		data,
		dbfsPath,
		data.Get("source").(string),
	)
}

func resourceDBFSRead(data *schema.ResourceData, client interface{}) error {
	isDir, fileSize, err := client.(*db.Client).DBFS().GetStatus(
		context.Background(),
		data.Id(),
	)
	if err != nil {
		return err
	}
	if !isDir {
		data.Set("file_size", int(fileSize))
	}
	data.Set("is_directory", isDir)

	return nil
}

func resourceDBFSUpdate(data *schema.ResourceData, client interface{}) error {
	// if it is a directory then it's a noop
	isDir, _, err := client.(*db.Client).DBFS().GetStatus(
		context.Background(),
		data.Id(),
	)
	if err != nil {
		return err
	}
	if isDir {
		return nil
	}

	// for updating a file it doesn't really make sense to do diffs, so we just
	// end up replacing it?
	err = client.(*db.Client).DBFS().Delete(
		context.Background(),
		data.Id(),
		false,
	)
	if err != nil {
		return err
	}

	return dbfsUploadFile(
		context.Background(),
		client,
		data,
		data.Id(),
		data.Get("source").(string),
	)
}

func resourceDBFSDelete(data *schema.ResourceData, client interface{}) error {
	return client.(*db.Client).DBFS().Delete(
		context.Background(),
		data.Id(),
		false,
	)
}

func dbfsUploadFile(
	ctx context.Context,
	client interface{},
	data *schema.ResourceData,
	dbfsPath, source string,
) error {
	handle, err := client.(*db.Client).DBFS().Create(
		ctx,
		dbfsPath,
		true,
	)
	if err != nil {
		return err
	}
	data.Set("is_directory", false)

	// check if the file is relative
	source = path.Clean(source)
	if strings.Index(source, "/") != 0 {
		wd, err := os.Getwd()
		if err != nil {
			return err
		}
		source = path.Join(wd, source)
	}

	// upload in <1MB chunks
	f, err := os.Open(source)
	if err != nil {
		return err
	}
	defer f.Close()

	fstat, err := f.Stat()
	if err != nil {
		return err
	}

	// guh... no int64 precision?
	data.Set("file_size", int(fstat.Size()))

	var bufSize int
	pos := int64(0)
	fsize := fstat.Size()
	if fsize < 900000 {
		bufSize = int(fsize)
	} else {
		bufSize = 900000
	}

	for {
		buf := make([]byte, bufSize)
		bytesRead, err := f.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = client.(*db.Client).DBFS().AddBlock(
			ctx,
			handle,
			buf,
		)
		if err != nil {
			return err
		}
		// should probably use f.Seek?
		pos += int64(bytesRead)
		if pos == fsize {
			break
		}
		// figure out the new bufSize
		if fsize-pos < 900000 {
			bufSize = int(fsize - pos)
		}
	}
	data.SetId(dbfsPath)

	return client.(*db.Client).DBFS().Close(ctx, handle)
}
