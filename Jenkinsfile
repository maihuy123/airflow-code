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
                sh 'python3 -m venv venv'
                sh '. venv/bin/activate && pip install flake8'
                sg '. venv/bin/activate && flake8'
            }
        }
    }
    
}