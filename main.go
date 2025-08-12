package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

type RedisInstance struct {
	IP   string `json:"ip"`
	Port string `json:"port"`
}

type RedisCluster struct {
	Name      string          `json:"name"`
	Instances []RedisInstance `json:"instances"`
	Password  string          `json:"password"`
}

type RedisConfig struct {
	Clusters []RedisCluster `json:"clusters"`
}

var sharedConfig *RedisConfig

func setupRedisClient(ctx context.Context, cluster *RedisCluster) (*redis.Client, error) {
	for _, instance := range cluster.Instances {
		opt := &redis.Options{
			Addr:     fmt.Sprintf("%s:%s", instance.IP, instance.Port),
			Password: cluster.Password,
			DB:       0,
		}
		client := redis.NewClient(opt)
		if err := client.Ping(ctx).Err(); err != nil {
			client.Close()
			continue
		}
		return client, nil
	}
	return nil, fmt.Errorf("failed to connect to any Redis instance in cluster %s", cluster.Name)
}

func getRedisInfo(ctx context.Context, client *redis.Client) (string, error) {
	res := client.Info(ctx)
	if res.Err() != nil {
		return "", fmt.Errorf("failed to get Redis info: %v", res.Err())
	}
	return res.String(), nil
}

func parseRedisInfo(info string) map[string]string {
	m := make(map[string]string)
	sc := bufio.NewScanner(strings.NewReader(info))
	for sc.Scan() {
		line := sc.Text()
		// Remove info because go-redis client outputs info: at the first line somehow.
		if len(line) == 0 || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "info:") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		m[parts[0]] = parts[1]
	}
	return m
}

func getReplicas(infoMap map[string]string) [][2]string {
	replicas := [][2]string{}
	for i := range 5 {
		k := fmt.Sprintf("slave%d", i)
		if v, ok := infoMap[k]; ok && v != "" {
			var ip, port string
			for _, item := range strings.Split(v, ",") {
				parts := strings.SplitN(item, "=", 2)
				if len(parts) != 2 {
					continue
				}
				switch parts[0] {
				case "ip":
					ip = parts[1]
				case "port":
					port = parts[1]
				}
			}
			if ip != "" && port != "" {
				replicas = append(replicas, [2]string{ip, port})
			}
		}
	}
	return replicas
}

func getClusterMasters(ctx context.Context, client *redis.Client) [][2]string {
	res := client.Do(ctx, "CLUSTER", "NODES")
	if res.Err() != nil {
		return nil
	}
	nodes := res.String()
	masters := [][2]string{}
	for _, node := range strings.Split(nodes, "\n") {
		// Remove CLUSTER NODES: because go-redis client outputs it at the first line somehow.
		node = strings.TrimPrefix(node, "CLUSTER NODES:")
		node = strings.TrimSpace(node)
		if node == "" {
			continue
		}
		items := strings.Fields(node)
		if len(items) < 3 {
			continue
		}
		if !strings.Contains(items[2], "master") {
			continue
		}
		addr := strings.Split(items[1], "@")[0]
		ipPort := strings.Split(addr, ":")
		if len(ipPort) == 2 {
			masters = append(masters, [2]string{ipPort[0], ipPort[1]})
		}
	}
	return masters
}

func collectReplicasInfo(ctx context.Context, cluster *RedisCluster, masterInfo map[string]string, result *[]map[string]string) {
	cnt, _ := strconv.Atoi(masterInfo["connected_slaves"])
	if cnt <= 0 {
		return
	}
	for _, replica := range getReplicas(masterInfo) {
		replicaCluster := RedisCluster{
			Name: cluster.Name,
			Instances: []RedisInstance{
				{
					IP:   replica[0],
					Port: replica[1],
				},
			},
			Password: cluster.Password,
		}
		if replicaCon, err := setupRedisClient(ctx, &replicaCluster); err == nil {
			if replicaInfo, err := getRedisInfo(ctx, replicaCon); err == nil {
				replicaInfoMap := parseRedisInfo(replicaInfo)
				replicaInfoMap["ip"] = replica[0]
				*result = append(*result, replicaInfoMap)
			}
			_ = replicaCon.Close()
		}
	}
}

func collectClusterData(ctx context.Context, name string) []map[string]string {
	var cluster *RedisCluster
	for i := range sharedConfig.Clusters {
		if sharedConfig.Clusters[i].Name == name {
			cluster = &sharedConfig.Clusters[i]
			break
		}
	}
	if cluster == nil {
		return []map[string]string{
			{"error": "Cluster not found"},
		}
	}

	con, err := setupRedisClient(ctx, cluster)
	if err != nil {
		return []map[string]string{
			{"error": err.Error()},
		}
	}
	defer con.Close()

	info, err := getRedisInfo(ctx, con)
	if err != nil {
		return []map[string]string{
			{"error": err.Error()},
		}
	}

	infoMap := parseRedisInfo(info)
	clusterEnabled := infoMap["cluster_enabled"] == "1"

	var result []map[string]string

	if clusterEnabled {
		masters := getClusterMasters(ctx, con)
		for _, master := range masters {
			masterCluster := RedisCluster{
				Name: cluster.Name,
				Instances: []RedisInstance{
					{
						IP:   master[0],
						Port: master[1],
					},
				},
				Password: cluster.Password,
			}
			if masterCon, err := setupRedisClient(ctx, &masterCluster); err == nil {
				if masterInfo, err := getRedisInfo(ctx, masterCon); err == nil {
					masterInfoMap := parseRedisInfo(masterInfo)
					masterInfoMap["ip"] = master[0]
					result = append(result, masterInfoMap)
					collectReplicasInfo(ctx, &masterCluster, masterInfoMap, &result)
				}
				_ = masterCon.Close()
			}
		}
	} else {
		role := strings.ToLower(infoMap["role"])
		switch role {
		case "master":
			infoMap["ip"] = cluster.Instances[0].IP
			result = append(result, infoMap)
			collectReplicasInfo(ctx, cluster, infoMap, &result)
		case "slave":
			masterHost := infoMap["master_host"]
			masterPort := infoMap["master_port"]
			if masterHost != "" && masterPort != "" {
				masterCluster := RedisCluster{
					Name:      cluster.Name,
					Instances: []RedisInstance{{IP: masterHost, Port: masterPort}},
					Password:  cluster.Password,
				}
				if masterCon, err := setupRedisClient(ctx, &masterCluster); err == nil {
					if mi, err := getRedisInfo(ctx, masterCon); err == nil {
						mInfo := parseRedisInfo(mi)
						mInfo["ip"] = masterHost
						result = append(result, mInfo)
						collectReplicasInfo(ctx, &masterCluster, mInfo, &result)
					}
					_ = masterCon.Close()
				}
			}
		}
	}
	return result
}

func sseHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data := collectClusterData(ctx, name)
			b, _ := json.Marshal(data)
			fmt.Fprintf(w, "data: %s\n\n", string(b))
			flusher.Flush()
		}
	}
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.ParseFiles("templates/index.html"))
	_ = tmpl.Execute(w, nil)
}

func clustersJSONHandler(w http.ResponseWriter, r *http.Request) {
	names := make([]string, 0, len(sharedConfig.Clusters))
	for _, cluster := range sharedConfig.Clusters {
		names = append(names, cluster.Name)
	}
	sort.Strings(names)
	type clusterName struct {
		Name string `json:"name"`
	}
	res := struct {
		Clusters []clusterName `json:"clusters"`
	}{
		Clusters: make([]clusterName, 0, len(sharedConfig.Clusters)),
	}
	for _, name := range names {
		res.Clusters = append(res.Clusters, clusterName{Name: name})
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(res)
}

func namedIndexHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	tmpl := template.Must(template.ParseFiles("templates/cluster.html"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	data := struct {
		ClusterName string
	}{
		ClusterName: name,
	}
	_ = tmpl.Execute(w, data)
}

func printUsageAndExit(code int) {
	fmt.Fprintln(os.Stderr, `
Usage:
  go run . <config.json>
  stormy <config.json>

Example+
  go run . stormy-config.json
	`)
	os.Exit(code)
}

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit(2)
	}
	if os.Args[1] == "-h" || os.Args[1] == "--help" {
		printUsageAndExit(0)
	}
	cfgPath := os.Args[1]
	f, err := os.Open(cfgPath)
	if err != nil {
		log.Fatalf("Failed to read config file '%s': '%v'", cfgPath, err)
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&sharedConfig); err != nil {
		log.Fatalf("Failed to parse config file '%s': '%v'", cfgPath, err)
	}

	router := mux.NewRouter()
	router.HandleFunc("/", indexHandler).Methods(http.MethodGet)
	router.HandleFunc("/clusters.json", clustersJSONHandler).Methods(http.MethodGet)
	router.HandleFunc("/{name}", namedIndexHandler).Methods(http.MethodGet)
	router.HandleFunc("/{name}/events", sseHandler).Methods(http.MethodGet)

	addr := "127.0.0.1:8080"
	log.Printf("Listening on http://%s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
