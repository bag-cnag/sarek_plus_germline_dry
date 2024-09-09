def getSchemaFromGitea(branch, credentials,project_folder) {
               
               sh "rm -r -f "+project_folder
               withCredentials([string(credentialsId: credentials, variable: 'gitea_token')]) {
    // some block

               
            sh "git clone -c http.sslVerify=false -b " + "develop" + " https://" + gitea_token + ":x-oauth-basic@gitea.gpapdev.cnag.eu/gitea/platform/"+project_folder
             sh "cd "+project_folder+" && git fetch --all"
                
                try {
                    sh "cd "+project_folder+" && git checkout -b " + branch
                    }
                catch(Exception e1) {
                        sh "cd "+project_folder+" && git checkout  " + branch
                 println(e1);
               //Catch block 
                   }       
                try {
                    sh "cd "+project_folder+" && git pull origin " + branch 
                    }
                catch(Exception e1) {
                 println(e1);
   //Catch block 
                }
        
        //sh "cp -r "+project_folder+"/formSchema/UIconfig.js ./phenostore_server/config/."        
            
               }
                   
               }

def getConfigFromGitea(branch, credentials,project_folder) {
               
               sh "rm -r -f "+project_folder
               withCredentials([string(credentialsId: credentials, variable: 'gitea_token')]) {
    // some block

               
             sh "git clone -c http.sslVerify=false -b " + "develop" + " https://" + gitea_token + ":x-oauth-basic@gitea.gpapdev.cnag.eu/gitea/platform/" +project_folder
             sh "cd "+project_folder+" && git fetch --all"
                
                try {
                    sh "cd "+project_folder+" && git checkout -b " + branch
                    }
                catch(Exception e1) {
                        sh "cd "+project_folder+" && git checkout  " + branch
                 println(e1);
               //Catch block 
                   }       
                try {
                    sh "cd "+project_folder+" && git pull origin " + branch 
                    }
                catch(Exception e1) {
                 println(e1);
   //Catch block 
                }
        
        //sh "cp -r "+project_folder+"/config_test.py ./phenostore_server/config/."
        sh "cp -r "+project_folder+"/config.py ./server"
        
            
               }
                   
               }
 
def BuildAndCopyMibsHere(branch, credentials,project_folder,content) {
               
               sh "rm -r -f "+project_folder
               withCredentials([string(credentialsId: credentials, variable: 'gitea_token')]) {
    // some block

               
            sh "git clone -c http.sslVerify=false -b " + "develop" + " https://" + gitea_token + ":x-oauth-basic@gitea.gpapdev.cnag.eu/gitea/platform/"+project_folder+" "+project_folder
             sh "cd "+project_folder+" && git fetch --all"
                
                try {
                    sh "cd "+project_folder+" && git checkout -b " + branch
                    }
                catch(Exception e1) {
                        sh "cd "+project_folder+" && git checkout  " + branch
                 println(e1);
               //Catch block 
                   }       
                try {
                    sh "cd "+project_folder+" && git pull origin " + branch 
                    }
                catch(Exception e1) {
                 println(e1);
   //Catch block 
                }
        
        sh "cp "+content+" "+project_folder+"/."
        sh "cd "+project_folder+" && if [ \$(git status --porcelain | wc -l) -gt 0 ]; then git add * && git commit -m 'Latest build' -i * && git push origin " + branch + "; else echo 'No chanegs to commit'; fi"
            
               }
                   
               }





    pipeline {
        agent any
        stages {
            stage('install requirements') {
                steps {
                    
                    withPythonEnv('python3'){
    sh 'pip install pip==22.1.1'
    sh 'pip install -r requirements.txt'
}
                }
            }
            stage('run test') {
                steps {
                    
                    withPythonEnv('python3'){
                        withCredentials([string(credentialsId: 'playground_password', variable: 'password')]) {
                            sh 'export username=test'
                            sh 'pytest'
                        //sh 'docker run --net=host --name postgres_test_genomed -e POSTGRES_PASSWORD=mysecretpassword -d postgres'
                        //sh "rm -rf federate_central_server_config"
                        //getConfigFromGitea(env.BRANCH_NAME,"gitea_config","federate_central_server_config")
                        //sh 'pytest --cov-report xml:phenostore_server/tests/coverage.xml --cov=. phenostore_server/tests/test_*'
                        //sh 'rm -r -f phenostore_server/attachments/rdconnect'
                        //sh 'docker rm -f '+'$(docker ps -aqf "name=postgres_test_flask")'

                        }

                    }
                }
            }
          
          /* stage('Sonarqube') {
              
            environment {
                scannerHome = tool 'SQScanner'
                SONAR_SCANNER_OPTS = '-Djavax.net.ssl.trustStore=/home/ujenkins/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044'
            }
                
            steps {
                withSonarQubeEnv('SQServer') {
                withCredentials([string(credentialsId: 'sonar_token', variable: 'sonar_token')]) {
                
                sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=phenostore_server -Dsonar.login=${sonar_token}"
                }
              }
            }
         }

         stage("Quality Gate") {
       
            environment {
                scannerHome = tool 'SQScanner'
                SONAR_SCANNER_OPTS = '-Djavax.net.ssl.trustStore=/home/ujenkins/cacerts -Djavax.net.ssl.trustStorePassword=changeit -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044'
            }
            
            steps {
            timeout(time: 15, unit: 'MINUTES') {
                waitForQualityGate abortPipeline: true
                script{
                    def qg = waitForQualityGate()
                    if(qg.status != 'OK'){
                        error "Pipeline aborted due to quality gate failure: ${qg.status}"
                    }
                }
            }
         }
        } */
      

        }

    post {


              success {

                  slackSend color: "good", message: "Job: ${env.JOB_NAME} with buildnumber ${env.BUILD_NUMBER} was successful"
              }

              failure {
                //sh 'docker rm -f '+'$(docker ps -aqf "name=postgres_test_genomed")'
                slackSend color: "danger", message: "Job: ${env.JOB_NAME} with buildnumber ${env.BUILD_NUMBER} was failed"

              }
    
 
        }
    

    }