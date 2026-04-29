"""환경변수 로드 및 설정"""
import os
from dotenv import load_dotenv

load_dotenv()

# 공공데이터 API
FACTORY_API_KEY = os.getenv("FACTORY_API_KEY", "")
VWORLD_API_KEY = os.getenv("VWORLD_API_KEY", "")

# Claude API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# 이메일
SMTP_EMAIL = os.getenv("SMTP_EMAIL", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "HELLIO BRIDGE")

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# 스케줄러
DAILY_COLLECT_HOUR = int(os.getenv("DAILY_COLLECT_HOUR", "6"))
DAILY_EMAIL_HOUR = int(os.getenv("DAILY_EMAIL_HOUR", "9"))
MAX_EMAILS_PER_DAY = int(os.getenv("MAX_EMAILS_PER_DAY", "30"))

# 팩토리온 API
FACTORY_API_URL = "http://apis.data.go.kr/B550624/fctryRegistLndpclInfo/getFctryLndpclService"

# 최소 공장면적 (m²) — 태양광 설치 기준 (200평 = 660m²)
MIN_FACTORY_AREA = 660

# Google Custom Search API
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GOOGLE_CX = os.getenv("GOOGLE_CX", "")

# DART 전자공시 API (이메일 수집용)
DART_API_KEY = os.getenv("DART_API_KEY", "")
