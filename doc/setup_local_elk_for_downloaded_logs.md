# Setup local Kibana ELK stack for local logs

You can set up a local ELK and import my local log files into it.
Thus, we would have a consistent experience on troubleshooting for both
SaaS and Self-managed customers.

1. Make sure `docker` and `Docker-compose` are installed.
1. Clone repo [Docker-elk](https://github.com/deviantony/docker-elk).
1. Run `docker-compose up setup`, this is to set up some prerequisites.
1. Run `docker-compose up`, this starts ELK UI at [localhost:5601](http://localhost:5601/app/home#/).
   - Default username: `elastic`
   - Default password: `changeme`
     ![elk-homepage.png](img%2Felk-homepage.png)

1. Import local log files.
   - Download a SOS package from a zendesk ticket
   - Unzip the package, and find the Gitaly log at `<un zip dir>/var/log/gitlab/gitaly/current`
   - In Elastic UI, search keyword `upload`

     ![elk-upload.png](img%2Felk-upload.png)

   - Drag a Gitaly log file on the upload page. Actually, you can import whatever logs you want to analyze.

     ![elk-impor-preview.png](img%2Felk-impor-preview.png)

   - When importing files, there are something to notice:
     - In Override setting, use `time` or `@timestamp` as the time field, other may lead to empty result
       ![elk-override-settings.png](img%2Felk-override-settings.png)

     - If needed, put ticket number in the index to avoid intervention with other logs
   - Once the file is imported and the index is created, go to `Analytics/Discover` and choose your index
     (i.e. data views) to view you log

      ![elk-view-logs.png](img%2Felk-view-logs.png)

- [Analyze Large Log Files Using ELK](https://ahmedmusaad.com/analyse-large-log-files-using-elk/)
- [Docker-elk](https://github.com/deviantony/docker-elk)
