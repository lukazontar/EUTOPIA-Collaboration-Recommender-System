Name: eutopia-1
Machine Type: C2 series (compute optimized)
vCPUs: 16
Memory: 64 GB
Disk: 10 GB balanced persistent disk, 50 GB SSD persistent disk
OS: Debian GNU/Linux 12 (Bookworm)
Firewall: HTTP/HTTPS traffic not allowed, no load balancer health checks
Network: Default settings, no tier 1 networking performance

Google Cloud Secret Manager is used to store secrets but the scripts are currently not using it.

- TODO: I'm not sure this is crucial, because dbt will most likely be run locally and for the ORCID script that requires
  secrets, the data is already loaded.

I added a
principal [eutopia-recommender-system@collaboration-recommender.iam.gserviceaccount.com](mailto:eutopia-recommender-system@collaboration-recommender.iam.gserviceaccount.com)
to the Compute Engine
VM instance with `Owner` role.

To test permissions, I ran the following command:

 ```
 gcloud alpha bq datasets list
 ```

I installed GitHub CLI and configured it with the following command:

```
sudo apt-get update
sudo apt-get install -y git docker.io gh
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
gh auth login (Login with GitHub via SSH and authentication token, add SSH key to GitHub account)
mkdir development
cd development
git clone git@github.com:lukazontar/EUTOPIA-Collaboration-Recommender-System.git
```