// TTW Software Team
// Mathis Van Eetvelde
// 2021-present

// Modified by Aditya Karnam
// 2021
// Added file overwrite support

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/sethvargo/go-githubactions"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

const (
	scope                 = "https://www.googleapis.com/auth/drive"		// 404 scope override
	srcIdInput            = "srcId"
	dstIdInput            = "dstId"
	trashIdInput          = "trashId"
	credentialsInput      = "credentials"
)

func main() {
	// get base64 encoded credentials argument from action input
	credentials := os.Args[1]
	if credentials == "" {
		missingInput(credentialsInput)
	}
	// add base64 encoded credentials argument to mask
	githubactions.AddMask(credentials)

	// decode credentials to []byte
	decodedCredentials, err := base64.StdEncoding.DecodeString(credentials)
	if err != nil {
		githubactions.Fatalf(fmt.Sprintf("base64 decoding of 'credentials' failed with error: %v", err))
	}

	creds := strings.TrimSuffix(string(decodedCredentials), "\n")

	// add decoded credentials argument to mask
	githubactions.AddMask(creds)

	// fetching a JWT config with credentials and the right scope
	conf, err := google.JWTConfigFromJSON([]byte(creds), scope)
	if err != nil {
		githubactions.Fatalf(fmt.Sprintf("fetching JWT credentials failed with error: %v", err))
	}

	// instantiating a new drive service
	ctx := context.Background()
	svc, err := drive.New(conf.Client(ctx))
	if err != nil {
		log.Println(err)
	}

	srcId := os.Args[2]
	dstId := os.Args[3]
	trashId := os.Args[4]
	driveNum := os.Args[5]


	// #########################################################


	r, err := svc.Files.List().
		Q("'me' in owners").
		Fields("files(size),nextPageToken").
		OrderBy("name").
		PageSize(1000).
		IncludeItemsFromAllDrives(true).
		SupportsAllDrives(true).
		Do()
	if err != nil {
		log.Fatalf("Unable to retrieve files: %v", err)
	}


	about, err := svc.About.Get().Fields("storageQuota").Do()
	if err != nil {
		log.Fatalf("Unable to get quota: %v", err)
	}

	quota := about.StorageQuota
	var driveSize int64 = quota.Limit-quota.Usage


	// #########################################################


	body := fmt.Sprintf("'%s' in parents", srcId)
	r, err = svc.Files.List().
		Q(body).
		Fields("files(id,name,size),nextPageToken").
		OrderBy("name").
		PageSize(1000).
		IncludeItemsFromAllDrives(true).
		SupportsAllDrives(true).
		Do()
	if err != nil {
		log.Fatalf("Unable to retrieve files: %v", err)
	}

	fmt.Print("[drive-", driveNum, "]  ")
	if len(r.Files) == 0 {
		fmt.Println("No files left.")
	} else {
		fmt.Println("Files:")
		for _, i := range r.Files {
			if (i.Size <= driveSize) && (i.Size > 0) {
				copyFile, err := svc.Files.Get(i.Id).SupportsAllDrives(true).Do()
				if err == nil {
					copyFile.Parents = []string{dstId}
					copyFile.Id = ""

					_, err := svc.Files.Copy(i.Id, copyFile).SupportsAllDrives(true).Do()
					if err == nil {
						fmt.Printf("Copied %v (%vs)\n", i.Name, i.Id)
						driveSize -= copyFile.Size

						for ok := true; ok; ok = true {
							movedFile := drive.File{}
							_, err := svc.Files.Update(i.Id, &movedFile).
									AddParents(trashId).
									RemoveParents(srcId).
									SupportsAllDrives(true).
									Do()
							if err == nil {
								break;
								//log.Println(err)
								//log.Println(fmt.Sprintf("%v -- %v  [%d / %d]", err, i.Name, i.Size, driveSize))
							}
						}
					} else {
						//log.Println(err)
						//log.Println(fmt.Sprintf("%v -- %v  [%d / %d]", err, i.Name, i.Size, driveSize))
					}
				} else {
					//log.Println(err)
					//log.Println(fmt.Sprintf("%v -- %v  [%d / %d]", err, i.Name, i.Size, driveSize))
				}
			} else {
				//log.Println(fmt.Sprintf("File too large %v  [%d / %d]", i.Name, i.Size, driveSize))
			}
		}
	}
}

func missingInput(inputName string) {
	githubactions.Fatalf(fmt.Sprintf("missing input '%v'", inputName))
}
