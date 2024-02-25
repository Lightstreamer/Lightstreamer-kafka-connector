To start the Quick Start example:

1. cd to the root project and run:

   `./gradlew quickStart`

   to fill the deploy folder with the required staff for building Docker images.

2. From this directory, run:
 
   `docker compose up --build`

   to start the Docker compose stack.

3. To clean up the deploy dir, from the root project dir:
 
   `./gradlew cleanQuickStart`
 
