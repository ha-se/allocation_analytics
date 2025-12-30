-- Snowflake Git API Integration の設定
-- このSQLをSnowflake Web UIのWorksheetで実行してください
-- 注意: ACCOUNTADMINロールまたはCREATE INTEGRATION権限が必要です

CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/ha-se')
  ENABLED = TRUE;

-- 実行後、以下のコマンドで確認できます:
-- SHOW API INTEGRATIONS LIKE 'git_api_integration';
-- DESC API INTEGRATION git_api_integration;

