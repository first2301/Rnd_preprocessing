import subprocess

def run_command(command):
    """쉘 명령어를 실행하고 결과를 반환합니다."""
    result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
    return result.stdout.strip()

def git_add(files='.'):
    """파일을 Git 스테이징 영역에 추가합니다."""
    run_command(f'git add {files}')
    print(f'Added files: {files}')

def git_commit(message):
    """Git 커밋을 생성합니다."""
    run_command(f'git commit -m "{message}"')
    print(f'Commit with message: {message}')

def git_push(branch='main'):
    """Git 리포지토리에 푸시합니다."""
    run_command(f'git push origin {branch}')
    print(f'Pushed to branch: {branch}')

if __name__ == "__main__":
    # Git 자동화 작업 실행
    git_add()  # 변경된 모든 파일 추가
    git_commit("test")  # 커밋 메시지 입력
    git_push("master")  # 메인 브랜치로 푸시
