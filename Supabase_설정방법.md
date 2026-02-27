# Supabase 설정 (데이터 영구 보관)

한 번 올린 거래/캔들 데이터가 Streamlit Cloud에서도 유지되도록 Supabase를 연결하는 방법입니다.

---

## 1. Supabase 프로젝트 만들기

1. **https://supabase.com** 접속 → **Start your project** (GitHub로 로그인 가능).
2. **New project** 클릭.
3. **Organization** 선택(없으면 새로 생성), **Name** 예: `nasdaq-review`, **Database Password**는 본인만 아는 비밀번호로 설정 후 **Create new project**.
4. 1~2분 정도 기다리면 프로젝트가 생성됩니다.

---

## 2. 테이블 생성

1. 왼쪽 메뉴에서 **SQL Editor** 클릭.
2. **New query** 클릭.
3. 이 프로젝트의 **`supabase_schema.sql`** 파일 내용을 **전부 복사**해서 SQL Editor에 붙여넣기.
4. **Run** (또는 Ctrl+Enter) 실행.

"Success. No rows returned" 정도로 나오면 정상입니다.

---

## 3. URL과 Key 복사

1. 왼쪽 메뉴 **Project Settings** (톱니바퀴) 클릭.
2. **API** 메뉴로 이동.
3. 아래 두 값을 복사해 두세요.
   - **Project URL** (예: `https://xxxxx.supabase.co`)
   - **anon public** 키 (**Project API keys** 안에 있음, `anon` / public 표시된 긴 문자열)

---

## 4. Streamlit Cloud에 Secrets 넣기

1. **https://share.streamlit.io** 접속 → 로그인.
2. **My apps**에서 나스닥 리뷰 앱 선택.
3. 오른쪽 위 **⋮** (세 점) → **Settings** → **Secrets**.
4. 아래처럼 입력 (본인 URL/키로 바꾸세요):

```toml
SUPABASE_URL = "https://xxxxx.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...."
```

5. **Save** 후, 앱을 한 번 **재시작**(Reboot app) 합니다.

---

## 5. 확인

- 앱에서 **체결/청산 엑셀 업로드** → **매칭 분석 시작** 하면 거래가 저장됩니다.
- 페이지를 새로고침하거나 나중에 다시 들어와도 **과거 날짜·데이터가 그대로** 나오면 Supabase 연동이 된 것입니다.

---

## 로컬에서 Supabase 쓰고 싶을 때

로컬에서도 같은 DB를 쓰려면, 터미널에서 실행하기 전에 환경 변수를 설정합니다.

**Windows (PowerShell):**
```powershell
$env:SUPABASE_URL = "https://xxxxx.supabase.co"
$env:SUPABASE_KEY = "여기에_anon_키"
streamlit run app.py
```

**Windows (CMD):**
```cmd
set SUPABASE_URL=https://xxxxx.supabase.co
set SUPABASE_KEY=여기에_anon_키
streamlit run app.py
```

설정하지 않으면 기존처럼 로컬 `trades.db`(SQLite)만 사용합니다.
