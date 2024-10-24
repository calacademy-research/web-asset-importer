pipeline {
    agent any

    environment {
        BRANCH_NAME = "${env.CHANGE_BRANCH ?: env.BRANCH_NAME}"
        PARENT_PATH = "/var/jenkins_home/workspace"
    }

    stages {
        stage('Checkout') {
            steps {
                script {
                    // Checkout the branch related to the PR
                    checkout scm
                    sh "git fetch --all"
                    sh "git reset --hard origin/${BRANCH_NAME}"
                }
            }
        }

        stage('Find and Save Directory') {
            steps {
                script {
                    // Find the directory name containing the substring "allbranch_${BRANCH_NAME}" but not containing "@tmp"
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
                    // Copy non-tracked files from web-asset-importer-ci to the found directory
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/settings.py ${env.FOUND_DIR}/settings.py"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/importer_jenkins_config.sh ${env.FOUND_DIR}/importer_jenkins_config.sh"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/server_host_settings.py ${env.FOUND_DIR}/server_host_settings.py"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/PIC_dbcreate/run_picdb.sh ${env.FOUND_DIR}/PIC_dbcreate/run_picdb.sh"
                    sh "cp -f ${PARENT_PATH}/web-asset-importer-ci/tests/casbotany_lite.db ${env.FOUND_DIR}/tests/casbotany_lite.db"
                    sh "cp -f -r ${PARENT_PATH}/web-asset-importer-ci/config_files ${env.FOUND_DIR}/"
                    sh "cp -f -r ${PARENT_PATH}/web-asset-importer-ci/html_reports ${env.FOUND_DIR}/"
                }
            }
        }

        stage('Run Importer setup') {
            steps {
                script {
                    // Run the provided shell script as root with timelimit
                    sh "chmod +x ${env.FOUND_DIR}/importer_jenkins_setup.sh && chmod +x ${env.FOUND_DIR}/importer_jenkins_config.sh"
                    timeout(time: 10, unit: 'MINUTES') {
                        sh "cd ${env.FOUND_DIR} && ./importer_jenkins_setup.sh"
                    }
                }
            }
        }

        stage('Run test Importers') {
            steps {
                script {
                    // Run the provided shell script as root with timelimit
                    sh "chmod +x ${env.FOUND_DIR}/importer_jenkins_run.sh"
                    timeout(time: 10, unit: 'MINUTES') {
                        sh "cd ${env.FOUND_DIR} && ./importer_jenkins_run.sh"
                    }
                }
            }
        }
    }
}
