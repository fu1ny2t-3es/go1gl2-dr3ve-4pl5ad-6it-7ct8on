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
	scope                 = "https://www.googleapis.com/auth/drive"
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

	r, err := svc.Files.List().
		Q("'me' in owners").
		Fields("files(id,name,size),nextPageToken").
		OrderBy("name").
		PageSize(1000).
		IncludeItemsFromAllDrives(true).
		SupportsAllDrives(true).
		Do()
	if err != nil {
		log.Fatalf("Unable to retrieve files: %v", err)
	}

	if os.Args[2] != "" {
		fmt.Print("[", os.Args[2], "]  ");
	}

	fmt.Println("Files:")
	if len(r.Files) != 0 {
		for _, i := range r.Files {
			fmt.Printf("%v (%v)\n", i.Name, i.Id)
		}
	}


	about, err := svc.About.Get().Fields("storageQuota").Do()
	if err != nil {
		log.Fatalf("Unable to get quota: %v", err)
	}

	quota := about.StorageQuota

	fmt.Printf("Used: %.2f\n", float32(quota.Usage) / 1024.0 / 1024.0 / 1024.0)
	fmt.Printf("Free: %.2f\n", float32(quota.Limit-quota.Usage) / 1024.0 / 1024.0 / 1024.0)
	fmt.Printf("Total: %.2f\n", float32(quota.Limit) / 1024.0 / 1024.0 / 1024.0)

	if quota.Usage >= quota.Limit {
		fmt.Printf("###  Warning: Free up drive space  ###")
	}

	fmt.Printf("\n\n")
}

func missingInput(inputName string) {
	githubactions.Fatalf(fmt.Sprintf("missing input '%v'", inputName))
}
