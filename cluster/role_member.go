package cluster

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ValidatePermission validates cluster permissions aka R or RW
func (cluster *ClusterNode) ValidatePermission(perm string) bool {
	switch perm {
	case "R":
		return true
	case "RW":
		return true
	default:
		return false
	}
}

// NewUser creates new database user
func (cluster *ClusterNode) NewUser(username, password, permission string) (string, map[string]interface{}, error) {
	user := make(map[string]interface{}) // Create map with username, password, and permission
	user["username"] = username
	user["password"] = password
	var encodeUsername string

	encodeUsername = base64.StdEncoding.EncodeToString([]byte(username))

	for _, u := range cluster.Config.Users {
		if strings.Split(u, ":")[0] == encodeUsername {
			return "", user, errors.New(fmt.Sprintf("%d Database user already exists.", 103))
		}
	}

	permission = strings.TrimSpace(strings.TrimSuffix(permission, ";")) // trim any space

	// validate permission
	if cluster.ValidatePermission(permission) {
		user["permission"] = permission
		b, err := json.Marshal(user)
		if err != nil {
			return "", user, errors.New(fmt.Sprintf("%d Could not marshal user for creation %s", 205, err.Error()))
		}

		h := sha256.New()
		h.Write(b)
		hashedUser := h.Sum(nil)

		cluster.ConfigMu.Lock()
		cluster.Config.Users = append(cluster.Config.Users, fmt.Sprintf("%s:%s", encodeUsername, base64.StdEncoding.EncodeToString(hashedUser))) // base64-encoded-username:struct-encoded-hashed
		cluster.ConfigMu.Unlock()

		return base64.StdEncoding.EncodeToString(hashedUser), user, nil
	} else {
		return "", user, errors.New(fmt.Sprintf("%d Invalid permission.", 101))
	}
}

// AuthenticateUser checks if a user exists and returns the user
func (cluster *ClusterNode) AuthenticateUser(username string, password string) (string, map[string]interface{}, error) {
	// Here we are hashing the provided username and password and seeing if a user exists with either permission

	userR := make(map[string]interface{}) // Create map with username, password, and permission
	userR["username"] = username
	userR["password"] = password
	userR["permission"] = "R"

	bR, err := json.Marshal(userR)
	if err != nil {
		panic(err)
	}
	hR := sha256.New()
	hR.Write(bR)
	hashedR := hR.Sum(nil)

	userRW := make(map[string]interface{}) // Create map with username, password, and permission
	userRW["username"] = username
	userRW["password"] = password
	userRW["permission"] = "RW"

	bRW, err := json.Marshal(userRW)
	if err != nil {
		return "", userRW, errors.New(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
	}
	hRW := sha256.New()
	hRW.Write(bRW)
	hashedRW := hRW.Sum(nil)

	for _, u := range cluster.Config.Users {
		if u == fmt.Sprintf("%s:%s", base64.StdEncoding.EncodeToString([]byte(username)), base64.StdEncoding.EncodeToString(hashedR)) {
			return u, userR, nil
		} else if u == fmt.Sprintf("%s:%s", base64.StdEncoding.EncodeToString([]byte(username)), base64.StdEncoding.EncodeToString(hashedRW)) {
			return u, userRW, nil
		}

	}

	return "", nil, errors.New(fmt.Sprintf("%d User does not exist.", 102))
}

// RemoveUser removes a user by username
func (cluster *ClusterNode) RemoveUser(username string) error {

	// Check if there's only 1 user.  If there is don't allow removal of the sole user.
	if len(cluster.Config.Users) == 1 {
		return errors.New(fmt.Sprintf("%d There must always be one database user available.", 204))
	}

	username = strings.TrimSuffix(username, ";")

	for j := 0; j < 50; j++ { // retry as gorm will serialize the bytes a bit different sometimes
		encodeUsername := base64.StdEncoding.EncodeToString([]byte(username))
		for i, user := range cluster.Config.Users {
			if strings.Split(user, ":")[0] == encodeUsername {
				cluster.ConfigMu.Lock() // Always lock config when modifying it for concurrent client cases
				cluster.Config.Users[i] = cluster.Config.Users[len(cluster.Config.Users)-1]
				cluster.Config.Users[len(cluster.Config.Users)-1] = ""
				cluster.Config.Users = cluster.Config.Users[:len(cluster.Config.Users)-1]
				cluster.ConfigMu.Unlock()
				return nil
			}
		}
	}

	return errors.New(fmt.Sprintf("%d No user found %s.", 102, username))
}
