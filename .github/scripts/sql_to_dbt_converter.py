#!/usr/bin/env python3
"""
SQL to dbt Model Converter
Scans a source repository for SQL queries used in analytics and converts them to dbt models.
Uses GitHub Models API (GPT-4) for conversions.
"""

import os
import re
import json
import shutil
import time
from pathlib import Path
from typing import List, Dict, Tuple
from openai import OpenAI

class SQLToDbtConverter:
    def __init__(self, source_repo_path: str, dbt_repo_path: str, target_dir: str, 
                 dbt_models_dir: str, github_token: str, model_token: str, source_repository: str = None):
        # Both repos are checked out as siblings in the GitHub Actions workspace
        # The script runs from inside dbt-repo, so we need to go up to access source-repo
        workspace_root = Path.cwd().parent
        
        # Path to the repository to scan
        self.source_repo_path = workspace_root / source_repo_path
        if target_dir == '.':
            self.target_dir = self.source_repo_path
        else:
            self.target_dir = self.source_repo_path / target_dir
        
        # Path to the dbt repository (where models will be created)
        # Since we're already in dbt-repo, use current directory
        self.dbt_repo_path = Path.cwd()
        self.dbt_models_dir = self.dbt_repo_path / dbt_models_dir
        
        # GitHub Models API client (uses OpenAI SDK with GitHub endpoint)
        self.client = OpenAI(
            base_url="https://models.github.ai/inference",
            api_key=model_token
        )
        self.source_repository = source_repository or "unknown"
        
        # Patterns that suggest analytics usage
        self.analytics_patterns = [
            r'analytics',
            r'metrics',
            r'reporting',
            r'dashboard',
            r'kpi',
            r'bi_',
            r'report_',
            r'aggregate',
            r'summary',
            r'stats',
            r'trend'
        ]
        
        # File extensions to scan
        self.scan_extensions = ['.py', '.js', '.ts', '.java', '.rb', '.php', '.sql', '.go']
    
    def find_sql_queries(self) -> List[Dict]:
        """Scan repository for SQL queries in analytics contexts."""
        sql_queries = []
        
        print(f"Current working directory: {Path.cwd()}")
        print(f"Source repository path: {self.source_repo_path}")
        print(f"Target scan directory: {self.target_dir}")
        print(f"dbt repository path: {self.dbt_repo_path}")
        print(f"dbt models output directory: {self.dbt_models_dir}")
        print()
        
        if not self.source_repo_path.exists():
            print(f"Error: Source repository path does not exist: {self.source_repo_path}")
            print(f"Available directories in parent: {list(self.source_repo_path.parent.iterdir())}")
            return []
        
        if not self.target_dir.exists():
            print(f"Error: Target directory does not exist: {self.target_dir}")
            return []
        
        print(f"Scanning for SQL queries in: {self.target_dir}")
        
        for file_path in self.target_dir.rglob('*'):
            if not file_path.is_file():
                continue
            
            if file_path.suffix not in self.scan_extensions:
                continue
            
            # Skip common excluded directories
            if any(part in file_path.parts for part in ['node_modules', '.git', 'venv', '__pycache__', 'dist', 'build']):
                continue
            
            try:
                content = file_path.read_text(encoding='utf-8')
                queries = self.extract_sql_from_file(file_path, content)
                sql_queries.extend(queries)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        
        print(f"Found {len(sql_queries)} potential analytics SQL queries")
        return sql_queries
    
    def extract_sql_from_file(self, file_path: Path, content: str) -> List[Dict]:
        """Extract SQL queries from a file."""
        queries = []
        
        # Pattern for SQL strings (handles multi-line and various quote styles)
        sql_patterns = [
            # Triple-quoted strings
            r'"""(.*?)"""',
            r"'''(.*?)'''",
            # Regular strings with SELECT
            r'"([^"]*SELECT[^"]*)"',
            r"'([^']*SELECT[^']*)'",
            # Template literals (JavaScript/TypeScript)
            r'`([^`]*SELECT[^`]*)`',
        ]
        
        for pattern in sql_patterns:
            matches = re.finditer(pattern, content, re.DOTALL | re.IGNORECASE)
            for match in matches:
                sql = match.group(1).strip()
                
                # Check if this looks like a SELECT query
                if not re.search(r'\bSELECT\b', sql, re.IGNORECASE):
                    continue
                
                # Check if this appears to be analytics-related
                if self.is_analytics_query(file_path, sql, content):
                    # Get relative path from source repo root
                    relative_path = file_path.relative_to(self.source_repo_path)
                    queries.append({
                        'sql': sql,
                        'file': f"{self.source_repository}/{relative_path}",
                        'relative_path': str(relative_path),
                        'context': self.get_query_context(content, match.start(), match.end())
                    })
        
        return queries
    
    def is_analytics_query(self, file_path: Path, sql: str, file_content: str) -> bool:
        """Determine if a SQL query is likely used for analytics."""
        # Check file path
        file_str = str(file_path).lower()
        if any(re.search(pattern, file_str) for pattern in self.analytics_patterns):
            return True
        
        # Check SQL content
        sql_lower = sql.lower()
        if any(pattern in sql_lower for pattern in ['group by', 'aggregate', 'sum(', 'count(', 'avg(']):
            return True
        
        # Check for analytics keywords in surrounding context
        context_lower = file_content.lower()
        if any(re.search(pattern, context_lower) for pattern in self.analytics_patterns):
            return True
        
        return False
    
    def get_query_context(self, content: str, start: int, end: int, context_lines: int = 5) -> str:
        """Get surrounding context for a query."""
        lines = content[:start].split('\n')
        start_line = max(0, len(lines) - context_lines)
        context = '\n'.join(lines[start_line:])
        return context[-500:] if len(context) > 500 else context  # Limit context size
    
    def convert_to_dbt_model(self, query_info: Dict) -> Dict:
        """Use GitHub Models (GPT-4) to convert SQL query to dbt model."""
        prompt = f"""Convert this SQL query into a dbt model. The query was found in: {query_info['file']}

Context around the query:
{query_info['context']}

SQL Query:
{query_info['sql']}

Please provide:
1. A clean, formatted SQL query suitable for a dbt model
2. A suggested model name (snake_case, descriptive)
3. Suggested materialization strategy (view, table, incremental)
4. Any recommended configurations (tags, schema, etc.)
5. A description for the model

Return your response as JSON with the following structure:
{{
    "model_name": "suggested_model_name",
    "sql": "formatted SQL for the model",
    "materialization": "view|table|incremental",
    "config": {{"tags": ["analytics"], "schema": "analytics"}},
    "description": "What this model does"
}}

Important:
- Remove any application-specific formatting or string concatenation
- Ensure the SQL is valid dbt/Jinja syntax
- Use dbt ref() and source() functions where appropriate
- Add clear comments for complex logic
"""
        
        # Retry logic with exponential backoff
        max_retries = 3
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model="openai/gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are an expert data engineer who converts SQL queries to dbt models."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=2000
                )
                
                # Extract JSON from response
                response_text = response.choices[0].message.content
                
                # Try to parse JSON from the response
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(0))
                else:
                    print(f"Could not parse JSON from GPT-4 response for {query_info['file']}")
                    return None
                    
            except Exception as e:
                error_str = str(e)
                
                # Check for rate limit errors
                if "rate" in error_str.lower() or "429" in error_str or "quota" in error_str.lower():
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                        print(f"⚠️  Rate limit hit. Waiting {wait_time}s before retry (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Rate limit error persists after {max_retries} retries")
                        print(f"   Error: {error_str}")
                        print(f"   Tip: Set MAX_QUERIES or increase DELAY_SECONDS")
                        return None
                
                # Other errors
                print(f"Error converting query from {query_info['file']}: {e}")
                return None
        
        return None
    
    def create_dbt_model_file(self, model_info: Dict, original_file: str):
        """Create a dbt model file from the conversion."""
        if not model_info:
            return
        
        model_name = model_info['model_name']
        sql = model_info['sql']
        config = model_info.get('config', {})
        description = model_info.get('description', '')
        
        # Create model directory if it doesn't exist
        self.dbt_models_dir.mkdir(parents=True, exist_ok=True)
        
        # Create the SQL model file
        model_file = self.dbt_models_dir / f"{model_name}.sql"
        
        # Build the model content with config block
        config_block = self.build_config_block(model_info)
        
        model_content = f"""{config_block}

/*
  Original source: {original_file}
  Description: {description}
*/

{sql}
"""
        
        model_file.write_text(model_content)
        print(f"Created dbt model: {model_file}")
        
        return str(model_file)
    
    def build_config_block(self, model_info: Dict) -> str:
        """Build the dbt config block."""
        materialization = model_info.get('materialization', 'view')
        config = model_info.get('config', {})
        
        config_lines = [
            "{{",
            "  config(",
            f"    materialized='{materialization}'"
        ]
        
        # Add additional config items
        if 'schema' in config:
            config_lines.append(f"    , schema='{config['schema']}'")
        
        if 'tags' in config:
            tags_str = json.dumps(config['tags'])
            config_lines.append(f"    , tags={tags_str}")
        
        config_lines.append("  )")
        config_lines.append("}}")
        
        return '\n'.join(config_lines)
    
    def create_schema_yml(self, models_created: List[str]):
        """Create a basic schema.yml file for the models."""
        schema_file = self.dbt_models_dir / "schema.yml"
        
        if schema_file.exists():
            print(f"Schema file already exists: {schema_file}")
            return
        
        schema_content = """version: 2

models:
"""
        
        for model_path in models_created:
            model_name = Path(model_path).stem
            schema_content += f"""  - name: {model_name}
    description: "Auto-generated analytics model"
    columns:
      - name: id
        description: "Primary identifier"

"""
        
        schema_file.write_text(schema_content)
        print(f"Created schema file: {schema_file}")
    
    def run(self):
        """Main execution flow."""
        print("Starting SQL to dbt conversion...")
        
        # Get rate limiting configuration from environment
        max_queries = int(os.getenv('MAX_QUERIES', '0'))  # 0 = unlimited
        delay_between_queries = float(os.getenv('DELAY_SECONDS', '1.0'))
        
        if max_queries > 0:
            print(f"⚠️  Limited to {max_queries} queries (set MAX_QUERIES env var to change)")
        if delay_between_queries > 0:
            print(f"⏱️  Delay between API calls: {delay_between_queries}s")
        print()
        
        # Find SQL queries
        queries = self.find_sql_queries()
        
        if not queries:
            print("No analytics SQL queries found.")
            return
        
        # Limit number of queries if specified
        if max_queries > 0 and len(queries) > max_queries:
            print(f"⚠️  Found {len(queries)} queries, but limiting to {max_queries}")
            queries = queries[:max_queries]
        
        print(f"Processing {len(queries)} queries...")
        print()
        
        # Convert each query with rate limiting
        models_created = []
        failed_queries = []
        
        for i, query_info in enumerate(queries, 1):
            print(f"Processing query {i}/{len(queries)} from {query_info['file']}")
            
            # Add delay between requests to avoid rate limits
            if i > 1 and delay_between_queries > 0:
                time.sleep(delay_between_queries)
            
            try:
                model_info = self.convert_to_dbt_model(query_info)
                
                if model_info:
                    model_file = self.create_dbt_model_file(model_info, query_info['file'])
                    if model_file:
                        models_created.append(model_file)
                        print(f"✓ Created model: {model_info['model_name']}")
                else:
                    failed_queries.append(query_info['file'])
                    print(f"✗ Failed to convert (check logs above)")
                    
            except Exception as e:
                failed_queries.append(query_info['file'])
                print(f"✗ Error: {e}")
            
            print()
        
        # Create schema.yml
        if models_created:
            self.create_schema_yml(models_created)
            print(f"\n✅ Successfully created {len(models_created)} dbt models")
        
        if failed_queries:
            print(f"\n⚠️  {len(failed_queries)} queries failed to convert:")
            for file in failed_queries[:10]:  # Show max 10
                print(f"   - {file}")
            if len(failed_queries) > 10:
                print(f"   ... and {len(failed_queries) - 10} more")
        
        if not models_created:
            print("\n⚠️  No models were created")


if __name__ == "__main__":
    # Get paths from environment
    source_repo_path = os.getenv('SOURCE_REPO_PATH', '.')
    dbt_repo_path = os.getenv('DBT_REPO_PATH', '.')
    target_dir = os.getenv('TARGET_DIR', '.')
    dbt_models_dir = os.getenv('DBT_MODELS_DIR', 'dbt/models/analytics')
    github_token = os.getenv('GITHUB_TOKEN')
    model_token = os.getenv('MODEL_TOKEN')
    source_repository = os.getenv('SOURCE_REPOSITORY', 'unknown')
    
    if not github_token:
        print("Error: GITHUB_TOKEN environment variable not set")
        print("This should be automatically provided by GitHub Actions")
        exit(1)
    
    print(f"Configuration:")
    print(f"  Source repository: {source_repository}")
    print(f"  Source repo path: {source_repo_path}")
    print(f"  dbt repo path: {dbt_repo_path}")
    print(f"  Target scan directory: {target_dir}")
    print(f"  dbt models output: {dbt_models_dir}")
    print(f"  Using GitHub Models API (GPT-4o)")
    print()
    
    converter = SQLToDbtConverter(
        source_repo_path=source_repo_path,
        dbt_repo_path=dbt_repo_path,
        target_dir=target_dir,
        dbt_models_dir=dbt_models_dir,
        github_token=github_token,
        model_token=model_token,
        source_repository=source_repository
    )
    converter.run()
