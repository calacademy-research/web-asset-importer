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
                }
            }
        }

        stage('Find and Save Directory') {
            steps {
                script {
                    // Find the directory name containing the substring "allbranch_${BRANCH_NAME}" but not containing "@tmp"
                    def dirName = sh(script: "find ${PARENT_PATH} -type d -name '*allbranch_${BRANCH_NAME}*' ! -name '*@tmp*' -print -quit", returnStdout: true).trim()
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
                    sh "cp ${PARENT_PATH}/web-asset-importer-ci/settings.py ${env.FOUND_DIR}/settings.py"
                    sh "cp ${PARENT_PATH}/web-asset-importer-ci/importer_jenkins.sh ${env.FOUND_DIR}/importer_jenkins.sh"
                    sh "cp ${PARENT_PATH}/web-asset-importer-ci/server_host_settings.py ${env.FOUND_DIR}/server_host_settings.py"
                    sh "cp ${PARENT_PATH}/web-asset-importer-ci/PIC_dbcreate/run_picdb.sh ${env.FOUND_DIR}/PIC_dbcreate/run_picdb.sh"
                    sh "cp ${PARENT_PATH}/web-asset-importer-ci/tests/casbotany_lite.db ${env.FOUND_DIR}/tests/casbotany_lite.db"
                    sh "cp -r ${PARENT_PATH}/web-asset-importer-ci/config_files ${env.FOUND_DIR}/"
                    sh "cp -r ${PARENT_PATH}/web-asset-importer-ci/html_reports ${env.FOUND_DIR}/"
                }
            }
        }

        stage('Run Script') {
            steps {
                script {
                    // Run the provided shell script as root with timelimit
                    timeout(time: 10, unit: 'MINUTES') {
                        sh "cd ${env.FOUND_DIR} && ./importer_jenkins.sh"
                    }
                }
            }
        }
    }
}
