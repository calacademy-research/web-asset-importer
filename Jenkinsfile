pipeline {
    agent any

    environment {
        BRANCH_NAME = "${env.CHANGE_BRANCH ?: env.BRANCH_NAME}"
        PARENT_PATH = "/var/jenkins_home/workspace"
        REPO_URL = "https://github.com/calacademy-research/web-asset-importer.git"
        LOCKFILE = "/tmp/importer_jenkins.lock"
    }

    stages {
        stage('Checkout') {
            steps {
                script {
                    if (env.CHANGE_ID) {
                        checkout([
                            $class: 'GitSCM',
                            branches: [[name: "FETCH_HEAD"]],
                            extensions: [],
                            userRemoteConfigs: [[
                                url: env.REPO_URL,
                                refspec: "+refs/pull/${env.CHANGE_ID}/head"
                            ]]
                        ])
                        sh "git checkout FETCH_HEAD"
                    } else {
                        checkout scm
                        sh "git fetch --all"
                        sh "git reset --hard origin/${BRANCH_NAME}"
                    }
                }
            }
        }

        stage('Find and Save Directory') {
            steps {
                script {
                    def dirName = "${WORKSPACE}"
                    if (dirName) {
                        env.FOUND_DIR = dirName
                    } else {
                        error "Directory containing 'allbranch_${BRANCH_NAME}' and not containing '@tmp' not found"
                    }
                }
            }
        }

        stage('Copy non-tracked files') {
            steps {
                script {
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/settings.py ${env.FOUND_DIR}/settings.py"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/importer_jenkins_config.sh ${env.FOUND_DIR}/importer_jenkins_config.sh"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/server_host_settings.py ${env.FOUND_DIR}/server_host_settings.py"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/PIC_dbcreate/run_picdb.sh ${env.FOUND_DIR}/PIC_dbcreate/run_picdb.sh"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/tests/casbotany_sqlite_create.sh ${env.FOUND_DIR}/tests/casbotany_sqlite_create.sh"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/tests/casbotany_lite.db ${env.FOUND_DIR}/tests/casbotany_lite.db"
                    sh "cp -f -r ${PARENT_PATH}/web-asset-importer-ci/config_files ${env.FOUND_DIR}/"
                    sh "cp -f -r ${PARENT_PATH}/web-asset-importer-ci/html_reports ${env.FOUND_DIR}/"
                }
            }
        }

        stage('Run Importer setup') {
            steps {
                script {
                    // Define the lockfile path
                    def lockFile = "${env.FOUND_DIR}/importer.lock"
                    def retries = 10
                    def waitTime = 30 // seconds

                    // Check for the lockfile and retry if it's present
                    while (fileExists(lockFile) && retries > 0) {
                        echo "Lockfile detected, waiting for ${waitTime} seconds before retrying..."
                        sleep(waitTime)
                        retries--
                    }

                    // If retries are exhausted, fail the build
                    if (retries == 0) {
                        error "Another instance of the script is already running. Timeout reached."
                    } else {
                        // No lockfile, proceed with running the setup script
                        echo "No lockfile found, proceeding with the setup script..."
                        sh "chmod +x ${env.FOUND_DIR}/importer_jenkins_setup.sh && chmod +x ${env.FOUND_DIR}/importer_jenkins_config.sh"
                        timeout(time: 10, unit: 'MINUTES') {
                            sh "cd ${env.FOUND_DIR} && ./importer_jenkins_setup.sh"
                        }
                    }
                }
            }
        }


        stage('Run test Importers') {
            steps {
                script {
                    sh "chmod +x ${env.FOUND_DIR}/importer_jenkins_run.sh"
                    timeout(time: 10, unit: 'MINUTES') {
                        sh "cd ${env.FOUND_DIR} && ./importer_jenkins_run.sh"
                    }
                }
            }
        }
    }

    post {
        always {
            // Clean up workspace with specified options
            cleanWs(
                cleanWhenNotBuilt: false,
                deleteDirs: true,
                disableDeferredWipeout: true,
                notFailBuild: true,
                patterns: [
                    [pattern: '.gitignore', type: 'INCLUDE'],
                    [pattern: '.propsfile', type: 'EXCLUDE']
                ]
            )
        }
    }
}