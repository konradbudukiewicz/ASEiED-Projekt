## ASEiED Project No.3

# Description
This AWS application analyzes height differences in the regions of Europe using data from https://registry.opendata.aws/terrain-tiles/ and plots them.

# Installation
Log into AWS, create a S3 bucket.  
Change the *fs.s3.access.key* and the *fsa.s3.secret.key* to your S3 bucket keys in *main.py*, lines 141, 144.  
Upload *main.py* and *install_python_modules.sh* to your S3 bucket.  
Create a cluster using ERM, preferred version is 6.4.0, add a bootstrap action that runs the *install_python_modules.sh*.  
SSH into the cluster and download the *main.py* from S3 bucket.  
Run the application using:
```
spark-submit main.py
```

# Usage
After a successful run, a map is generated on the S3 bucket, the map contains plotted terrain chunks of the highest parts of Europe.
Sample map below.
![Alt text](map.png?raw=true "Map")

# Credits
Created by:  
Mateusz-Witka Jeżewski  
Tomasz Walburg  
Maciej Baniukiewicz

