pipeline{
    agent any
    stages{
        stage('Clone stage'){
            steps{
                echo 'Hello world'
            }
            
        }
        stage('Code style check'){
            steps{
                sh 'pip install flake8'
                sg 'flake8'
            }
        }
    }
    
}