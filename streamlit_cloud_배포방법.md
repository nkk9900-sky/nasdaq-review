# 로컬 말고 클라우드에서 쓰기 (Streamlit Cloud)

로컬에서 차트가 안 나올 때, **Replit 말고** 브라우저로만 쓰는 방법입니다.

## 1. Streamlit Community Cloud (무료)

앱을 GitHub에 올린 뒤 Streamlit 서버에서 실행합니다. Replit처럼 클라우드에서 돌아가서 Yahoo 차트가 잘 나옵니다.

### 순서

1. **GitHub 계정**이 있으면 로그인, 없으면 가입 (무료).

2. **새 저장소 만들기**
   - https://github.com/new
   - Repository name: 예) `nasdaq-trade-review`
   - Public 선택 후 Create repository.

3. **이 프로젝트 폴더를 GitHub에 올리기**
   - 이 폴더(`나스닥 리뷰`) 안에서:
     - `실행.bat`, `.replit`, `.venv` 폴더는 올리지 않아도 됩니다.
     - 반드시 있어야 할 것: `app.py`, `database.py`, `kis_api.py`, `trade_classifier.py`, `trade_matcher.py`, `pdf_parser.py`, `main.py`, `requirements.txt`, `replit.md`, `pyproject.toml` 등 (`.venv` 제외).
   - GitHub 웹에서 "uploading an existing file"로 위 파일들 올리거나,
   - 로컬에서 Git 설치 후:
     ```bash
     cd "c:\나스닥 리뷰"
     git init
     git add app.py database.py kis_api.py trade_classifier.py trade_matcher.py pdf_parser.py main.py requirements.txt replit.md pyproject.toml .streamlit
     git commit -m "Initial commit"
     git branch -M main
     git remote add origin https://github.com/당신아이디/저장소이름.git
     git push -u origin main
     ```

4. **Streamlit Cloud에 배포**
   - https://share.streamlit.io 접속 → "Sign up with GitHub"로 로그인.
   - "New app" 클릭.
   - Repository: 방금 만든 저장소, Branch: `main`, Main file path: `app.py`.
   - "Deploy!" 클릭.
   - 1~2분 뒤 나오는 URL(예: `https://저장소이름-main-....streamlit.app`)로 접속하면 앱 실행됩니다.

5. **사용**
   - 브라우저에서 그 URL만 열면 됩니다. 로컬에서 실행할 필요 없습니다.
   - 체결/청산 엑셀은 "파일 업로드"로 올리면 됩니다.

### 한국투자증권 API 쓰려면

- Streamlit Cloud 대시보드 → 해당 앱 → Settings → Secrets에 아래처럼 추가:
  ```toml
  APP_KEY = "발급받은 키"
  APP_SECRET = "발급받은 시크릿"
  ```

---

## 2. 그냥 Replit에서만 쓰기

이미 Replit에서 잘 되니까, 계속 Replit에서만 쓰셔도 됩니다.  
소스는 Replit에 두고, 필요할 때만 Replit 탭 열어서 쓰는 방식입니다.

---

## 3. 다른 방법 (Hugging Face Spaces)

- https://huggingface.co/spaces 에서 새 Space 만들고, SDK: Streamlit 선택.
- 이 프로젝트 파일들을 올리고 `app.py`를 메인으로 실행하도록 설정하면, 역시 로컬이 아닌 클라우드에서 실행됩니다.

정리하면, **로컬 말고 쓰려면**  
1) Streamlit Cloud에 배포하거나,  
2) Replit에서만 쓰거나,  
3) Hugging Face Spaces에 올리면 됩니다.
