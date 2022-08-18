# Installing pgAdmin and Postgres on a Mac

SQL requires us to use a code editor with the ability to execute the scripts that are created by developers. This section guides you through the process of installing pgAdmin and Postgres on a Mac. 

## Before You Begin

* Remember to choose the installation package specific to your operating system and download the latest version.

* Be prepared to record your password—you will need it later!

## Download Link

* [PostgreSQL](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)

## Instructions

* After downloading PostgreSQL 12.11, double-click on the `postgresql-12.11-1-osx.dmg` file. 

* **Note:** The exact file version may be slightly different.

  ![””](Images/postgresql-12.11-1-osx.png)

* Go through the Setup Wizard and install PostgreSQL. Keep the default location `/Library/PostgreSQL/12`.

* Select the components to be installed. Uncheck the option to install Stack Builder.

  ![””](Images/stack_builder_mac.png)

* Add your data directory. Keep the default location `/Library/PostgreSQL/12/data`.

* Enter `postgres` as the password. **Be sure to record this password for future use.**

* Keep the default port as `5432`. In  the Advanced Options, set the locale as `[Default locale]`.

* The final screen will be the `Pre Installation Summary`.

* When the process is complete, you will have a folder in your `Applications` with these files.

  ![””](Images/PostgreSQL_folder.png)

* **Important:** if you are running the Big Sur update for Mac, be sure to download the latest version of pgAdmin.

  * Go to the [pgAdmin download](https://www.pgadmin.org/download/pgadmin-4-macos/) and select the latest version.

  * Click on the latest version `.dmg` file to start the download.

    ![pgAdmin4 latest version dmg files](Images/pgadmin4_versions.png)

  * Once the download is complete, click on the `.dmg` file in your downloads to install.

  * After installation has completed, drag the `pgAdmin` file into your applications folder. (This will take a few minutes.)

  * Once the transfer completes, you will be able to use pgAdmin. **Note:** You will still have a version in your PostgreSQL folder, but only use the version that you copied into `Applications`.

* To confirm the installation, start `pgAdmin` (it will open in a new browser window). Connect to the default server by clicking on it and entering the password if prompted.

- - - 

Copyright 2022 2U. All Rights Reserved.