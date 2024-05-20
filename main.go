package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"log"
	"os"
	"path"
	"strings"
)

type Cfg struct {
	StorageAccount string
	ContainerName  string
	UseCliAuth     bool
	AccountKey     string
}

func main() {
	cfg := Cfg{
		StorageAccount: "czcn57r03sa001dt",
		ContainerName:  "blackduckreport",
		UseCliAuth:     false,
		AccountKey:     "yVGY1wm5DbT7dZiR2sRLKZI1KiuPuhk0F5yRyyc66ejgRMrVaSjUFvJe+aU+1WNfgHJgoe15oE6R+AStZDz2Mg==",
	}
	ctx := context.Background()
	//filename := fmt.Sprintf("hello-%s.txt", time.Now().Format("20060102150405"))
	//file, err := os.Create(filename)
	//if _, err := file.WriteString("Written by Maxiaoyu"); err != nil {
	//	log.Fatal(err)
	//}
	//err = file.Close()
	//if err != nil {
	//	return
	//}
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//if err := Upload(ctx, cfg, filename); err != nil {
	//	log.Fatal(err)
	//}
	//
	//if err := os.Remove(filename); err != nil {
	//	log.Fatal(err)
	//}
	//
	//if err := Download(ctx, cfg, filename, "./"); err != nil {
	//	log.Fatal(err)
	//}

	blobs, err := List(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	for _, blob := range blobs {
		if strings.Contains(blob, "hello") {
			err := DeleteBlob(ctx, cfg, blob)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

}

// GetClient Authentication with azblob
func GetClient(accountName string, accountKey string, useCliAuth bool) (*azblob.Client, error) {
	url := fmt.Sprintf("https://%s.blob.core.chinacloudapi.cn/", accountName)
	if useCliAuth {
		credential, err := azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{})
		if err != nil {
			return nil, fmt.Errorf("NewAzureCLICredential error: %s", err)
		}
		return azblob.NewClient(url, credential, nil)
	}
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("NewSharedKeyCredential error: %s", err)
	}
	return azblob.NewClientWithSharedKeyCredential(url, credential, nil)
}

// List blobs
func List(ctx context.Context, cfg Cfg) ([]string, error) {
	client, err := GetClient(cfg.StorageAccount, cfg.AccountKey, cfg.UseCliAuth)
	if err != nil {
		return nil, err
	}
	var blobs []string
	pager := client.NewListBlobsFlatPager(cfg.ContainerName, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("NextPage error: %s", err)
		}
		for _, blob := range page.Segment.BlobItems {
			blobs = append(blobs, *blob.Name)
		}
	}
	return blobs, nil
}

// Upload upload file
func Upload(ctx context.Context, cfg Cfg, fileName string) error {
	client, err := GetClient(cfg.StorageAccount, cfg.AccountKey, cfg.UseCliAuth)
	if err != nil {
		return err
	}
	file, err := os.Open(fileName)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	if err != nil {
		return fmt.Errorf("Open error: %s", err)
	}
	_, err = client.UploadFile(ctx, cfg.ContainerName, fileName, file, nil)
	if err != nil {
		return fmt.Errorf("UploadFile error: %s", err)
	}
	return nil
}

func Download(ctx context.Context, cfg Cfg, blobName, destinationDir string) error {
	client, err := GetClient(cfg.StorageAccount, cfg.AccountKey, cfg.UseCliAuth)
	if err != nil {
		return err
	}
	target := path.Join(destinationDir, blobName)
	file, err := os.Create(target)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	if err != nil {
		return fmt.Errorf("create error: %s", err)
	}
	_, err = client.DownloadFile(ctx, cfg.ContainerName, blobName, file, nil)
	if err != nil {
		return fmt.Errorf("DownloadFile error: %s", err)
	}
	return nil
}

func DeleteBlob(ctx context.Context, cfg Cfg, blobName string) error {
	client, err := GetClient(cfg.StorageAccount, cfg.AccountKey, cfg.UseCliAuth)
	if err != nil {
		return err
	}
	_, err = client.DeleteBlob(ctx, cfg.ContainerName, blobName, nil)
	if err != nil {
		return fmt.Errorf("DeleteBlob error: %s", err)
	}
	fmt.Printf("blob %s is deleted\n", blobName)
	return nil
}
