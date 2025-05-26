from unittest import result
from dotenv import load_dotenv
load_dotenv()

import streamlit as st
import os
import psycopg2
from openai import OpenAI
import traceback
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import json


if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'current_query' not in st.session_state:
    st.session_state.current_query = None
if 'follow_up_active' not in st.session_state:
    st.session_state.follow_up_active = False
if 'follow_up_question' not in st.session_state:
    st.session_state.follow_up_question = ""

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


SCHEMA_KNOWLEDGE = {
    "weather_data": {
        "columns": {
            "city": "VARCHAR - Name of the city",
            "temperature": "FLOAT - Temperature in Celsius",
            "windspeed": "FLOAT - Wind speed in km/h",
            "winddirection": "FLOAT - Wind direction in degrees",
            "weathercode": "INT - Code representing weather condition",
            "timerecorded": "TIMESTAMP - When the data was recorded"
        },
        "sample_queries": [
            "SELECT city, temperature FROM weather_data WHERE temperature > 30 ORDER BY temperature DESC;",
            "SELECT city, AVG(temperature) as avg_temp FROM weather_data GROUP BY city HAVING AVG(temperature) > 20;",
            "SELECT city, COUNT(*) as records_count FROM weather_data GROUP BY city ORDER BY records_count DESC;"
        ]
    }
}


GEOGRAPHIC_KNOWLEDGE = {
    "africa": [
        'Nairobi', 'Kampala', 'Thika', 'Cairo', 'Johannesburg',
        'Lagos', 'Casablanca', 'Algiers', 'Accra', 'Kinshasa',
        'Addis Ababa', 'Dar es Salaam', 'Abidjan', 'Alexandria', 'Khartoum'
    ],
    "europe": [
        'London', 'Paris', 'Moscow', 'Berlin', 'Madrid',
        'Rome', 'Istanbul', 'Lisbon', 'Vienna', 'Prague'
    ],
    "asia": [
        'Beijing', 'Tokyo', 'Dubai', 'Singapore', 'Hong Kong',
        'Bangkok', 'Mumbai', 'Delhi', 'Shanghai', 'Seoul'
    ],
    "north_america": [
        'New York', 'Los Angeles', 'Chicago', 'Toronto', 'Vancouver',
        'Mexico City', 'Houston', 'Montreal', 'San Francisco', 'Miami'
    ],
    "south_america": [
        'Buenos Aires', 'Lima', 'Rio de Janeiro', 'Sao Paulo',
        'Bogota', 'Santiago', 'Caracas', 'Quito', 'Montevideo'
    ],
    "australia": [
        'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide'
    ]
}

class SQLAgent:
    def __init__(self):
        self.thinking_steps = []
        self.self_correction_attempts = 0
        self.max_self_correction_attempts = 3
        self.context_memory = []
        
    def log_thinking(self, step: str, content: str):
        """Record the agent's thought process"""
        self.thinking_steps.append({
            "timestamp": datetime.now().isoformat(),
            "step": step,
            "content": content
        })
    
    def add_to_memory(self, question: str, sql: str, results: List[Dict]):
        """Add current query to memory for future reference"""
        self.context_memory.append({
            "question": question,
            "sql": sql,
            "results_summary": str(results[:3]) if results else "No results",
            "timestamp": datetime.now().isoformat()
        })
    
    def get_context_summary(self) -> str:
        """Generate summary of previous queries for context"""
        if not self.context_memory:
            return "No previous queries in this session."
        
        summary = "Previous queries in this session:\n"
        for i, item in enumerate(self.context_memory, 1):
            summary += f"\n{i}. Question: {item['question']}\n   SQL: {item['sql']}\n   Results (sample): {item['results_summary']}\n"
        return summary
    
    def analyze_question(self, question: str) -> Dict:
        """Perform deep analysis of the user's question with context"""
        context = self.get_context_summary()
        
        analysis_prompt = f"""
        Analyze this weather data question deeply, considering this context:
        {context}
        
        Current question: {question}
        
        Detect if the question mentions any continents (Africa, Asia, Europe, etc.) or specific cities.
        
        Respond with a valid JSON object containing these fields:
        - intent: What the user wants to know
        - entities: List of important entities
        - timeframe: Any mentioned timeframe or null
        - comparisons: List of any comparisons
        - aggregations: List of any aggregation needs
        - language: Detected language
        - references_previous: Whether this references previous queries (true/false)
        - reference_details: How this relates to previous queries if applicable
        - geographic_scope: The continent mentioned if any (e.g., "Africa", "Asia")
        - mentioned_cities: List of specifically mentioned cities
        
        Return ONLY the JSON object, nothing else.
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a sophisticated question analyzer with context awareness. Respond with only a valid JSON object."},
                {"role": "user", "content": analysis_prompt}
            ]
        )
        
        try:
            analysis = json.loads(response.choices[0].message.content)
            self.log_thinking("Question Analysis", json.dumps(analysis, indent=2))
            return analysis
        except json.JSONDecodeError:
            
            return {
                "intent": "unknown",
                "entities": [],
                "timeframe": None,
                "comparisons": [],
                "aggregations": [],
                "language": "english",
                "references_previous": False,
                "reference_details": "",
                "geographic_scope": None,
                "mentioned_cities": []
            }
    
    def generate_sql_plan(self, analysis: Dict, question: str) -> str:
        """Create a step-by-step plan for SQL generation with context"""
        context = self.get_context_summary()
        
        plan_prompt = f"""
        Based on this analysis:
        {json.dumps(analysis, indent=2)}
        
        Previous queries context:
        {context}
        
        And this database schema:
        {SCHEMA_KNOWLEDGE}
        
        Available continents and their cities:
        {GEOGRAPHIC_KNOWLEDGE}
        
        Create a step-by-step plan to generate the SQL query. Consider:
        1. Which tables and columns are needed
        2. What filters/conditions to apply (including geographic filters if needed)
        3. Any sorting/grouping requirements
        4. How to handle the specific intent
        5. How this relates to previous queries if relevant
        6. How to handle any geographic scope mentioned
        
        Respond with clear numbered steps.
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL query planner with context awareness."},
                {"role": "user", "content": plan_prompt}
            ]
        )
        
        plan = response.choices[0].message.content
        self.log_thinking("SQL Generation Plan", plan)
        return plan
    
    def generate_initial_sql(self, plan: str, question: str, analysis: Dict) -> str:
        """Generate initial SQL based on the plan"""
        context = self.get_context_summary() if analysis.get("references_previous", False) else ""
       
        if analysis.get("geographic_scope"):
            continent = analysis["geographic_scope"].lower()
            if continent in GEOGRAPHIC_KNOWLEDGE:
                cities = GEOGRAPHIC_KNOWLEDGE[continent]
                city_list = ", ".join([f"'{city}'" for city in cities])
                
            
                if "average" in question.lower() or "avg" in question.lower():
                    sql_prompt = f"""
                    Based on this plan:
                    {plan}
                    
                    Previous queries context (if relevant):
                    {context}
                    
                    And the original question:
                    {question}
                    
                    Generate a PostgreSQL SQL query that calculates the average temperature 
                    for all cities in {continent} as a single value.
                    
                    The query should:
                    1. Filter for cities in {continent} only
                    2. Calculate the average temperature across all these cities
                    3. Not include city in the SELECT or GROUP BY since we want one aggregate value
                    
                    Example correct query:
                    SELECT AVG(temperature) as avg_temp FROM weather_data WHERE city IN ('Nairobi', 'Cairo', ...);
                    
                    Return ONLY the SQL query with no additional text, no markdown formatting, and no code block syntax.
                    The response must be executable SQL only.
                    """
                else:
                    
                    sql_prompt = f"""
                    Based on this plan:
                    {plan}
                    
                    Previous queries context (if relevant):
                    {context}
                    
                    And the original question:
                    {question}
                    
                    Generate a PostgreSQL SQL query for the weather_data table.
                    Apply this geographic filter: WHERE city IN ({city_list})
                    Return ONLY the SQL query with no additional text, no markdown formatting, and no code block syntax.
                    The response must be executable SQL only.
                    """
        else:
            
            sql_prompt = f"""
            Based on this plan:
            {plan}
            
            Previous queries context (if relevant):
            {context}
            
            And the original question:
            {question}
            
            Generate a PostgreSQL SQL query for the weather_data table.
            Return ONLY the SQL query with no additional text, no markdown formatting, and no code block syntax.
            The response must be executable SQL only.
            """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a PostgreSQL expert. Generate SQL only - no markdown, no code blocks, just pure SQL."},
                {"role": "user", "content": sql_prompt}
            ]
        )
        
        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        
        self.log_thinking("Initial SQL Generation", sql)
        return sql
    
    def validate_sql(self, sql: str) -> Tuple[bool, str]:
        """Validate the SQL before execution"""
        if "```" in sql:
            return False, "Query contains markdown code block syntax"
        
        validation_prompt = f"""
        Validate this PostgreSQL SQL query for syntax and logical correctness:
        {sql}
        
        The database has this schema:
        {SCHEMA_KNOWLEDGE}
        
        Available geographic knowledge:
        {GEOGRAPHIC_KNOWLEDGE}
        
        Respond in this exact format:
        VALID: true|false
        REASON: "reason for validation result"
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL validator. Check for syntax errors and logical consistency with the schema."},
                {"role": "user", "content": validation_prompt}
            ]
        )
        
        result = response.choices[0].message.content
        valid = "VALID: true" in result
        reason = result.split("REASON: ")[1].strip('"') if "REASON: " in result else "Unknown error"
        self.log_thinking("SQL Validation", f"Valid: {valid}\nReason: {reason}")
        return valid, reason
    
    def self_correct_sql(self, sql: str, validation_result: str, question: str) -> Optional[str]:
        """Attempt to self-correct invalid SQL"""
        if self.self_correction_attempts >= self.max_self_correction_attempts:
            return None
            
        correction_prompt = f"""
        The following SQL query was invalid:
        {sql}
        
        Validation error:
        {validation_result}
        
        Original question:
        {question}
        
        Database schema:
        {SCHEMA_KNOWLEDGE}
        
        Geographic knowledge:
        {GEOGRAPHIC_KNOWLEDGE}
        
        Please correct the SQL query. Return ONLY the corrected SQL with no additional text.
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a SQL corrector."},
                {"role": "user", "content": correction_prompt}
            ]
        )
        
        corrected_sql = response.choices[0].message.content.strip()
        self.self_correction_attempts += 1
        self.log_thinking(f"Self-Correction Attempt {self.self_correction_attempts}", corrected_sql)
        return corrected_sql
    
    def explain_query(self, sql: str, question: str, language: str, analysis: Dict) -> str:
        """Generate a sophisticated explanation of the query"""
        context = self.get_context_summary() if analysis.get("references_previous", False) else ""
        
        explanation_prompt = f"""
        The user asked (in {language}): "{question}"
        
        Context from previous queries:
        {context}
        
        The generated SQL query is:
        {sql}
        
        Database schema:
        {SCHEMA_KNOWLEDGE}
        
        Geographic context:
        {f"Continent: {analysis['geographic_scope']}" if analysis.get('geographic_scope') else ""}
        {f"Mentioned cities: {analysis['mentioned_cities']}" if analysis.get('mentioned_cities') else ""}
        
        Provide a detailed explanation in {language.capitalize()} covering:
        1. The technical approach taken
        2. Why this query answers the question
        3. Any important considerations or limitations
        4. How this relates to previous queries if relevant
        5. Alternative approaches that could have been taken
        
        Structure the explanation clearly with appropriate headings.
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": f"You explain SQL queries in {language} with technical depth and context awareness."},
                {"role": "user", "content": explanation_prompt}
            ]
        )
        
        return response.choices[0].message.content
    
    def generate_visualization_recommendation(self, sql: str, results: List[Dict]) -> str:
        """Recommend how to visualize the results"""
        recommendation_prompt = f"""
        Based on this SQL query:
        {sql}
        
        And these sample results (first 3 rows):
        {results[:3]}
        
        Recommend the best way to visualize this data. Consider:
        - Chart type (bar, line, pie, etc.)
        - What to put on each axis
        - Any important annotations
        - Color recommendations
        - Geographic aspects if relevant
        
        Provide a detailed recommendation with reasoning.
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a data visualization expert."},
                {"role": "user", "content": recommendation_prompt}
            ]
        )
        
        return response.choices[0].message.content
    
    def generate_follow_up_questions(self, question: str, sql: str, results: List[Dict]) -> List[str]:
        """Generate relevant follow-up questions"""
        prompt = f"""
        Based on this interaction:
        User question: {question}
        SQL query: {sql}
        Results sample: {results[:3] if results else "No results"}
        
        Generate 3-5 relevant follow-up questions the user might ask next.
        Consider geographic context if relevant.
        Return as a JSON list like this:
        ["question1", "question2", "question3"]
        """
        
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You generate insightful follow-up questions."},
                {"role": "user", "content": prompt}
            ]
        )
        
        try:
            return json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            return [
                f"What's the average temperature for cities in these results?",
                f"Can you show me the wind speed distribution?",
                f"How does this compare to data from last month?"
            ]

class DatabaseManager:
    def __init__(self):
        self.conn_params = {
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "dbname": os.getenv("PG_DATABASE")
        }
    
    def execute_query(self, query: str) -> Tuple[List[str], List[Tuple]]:
        """Execute SQL query and return results"""
        if "```" in query:
            raise ValueError("Query contains markdown syntax and cannot be executed")
        
        conn = psycopg2.connect(**self.conn_params)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.description:
                    colnames = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    return colnames, rows
                return [], []
        finally:
            conn.close()

def display_query_result(query_data):
    """Display a query result from history"""
    st.subheader(f"🔍 Query from {query_data['timestamp']}")
    
    with st.expander("View Details", expanded=True):
        st.markdown(f"**Question:** {query_data['question']}")
        
        st.markdown("**Generated SQL:**")
        st.code(query_data['sql'], language="sql")
        
        st.markdown("**Explanation:**")
        st.markdown(query_data['explanation'])
        
        if query_data['results']:
            st.markdown("**Results:**")
            st.dataframe(
                data=query_data['results'],
                use_container_width=True,
                hide_index=True,
                height=min(400, 35 * len(query_data['results']) + 3),
            )
            
            st.markdown("**Visualization Recommendation:**")
            st.markdown(query_data['visualization_rec'])
        else:
            st.info("No results found for this query.")
        
        if query_data['follow_ups']:
            st.markdown("**Suggested Follow-ups:**")
            for i, question in enumerate(query_data['follow_ups'], 1):
                st.markdown(f"{i}. {question}")

def main():
    st.set_page_config(
        page_title="Advanced Weather Data AI Agent with Memory",
        page_icon="🌍",
        layout="wide"
    )
    
    st.title("🌦️ Advanced Weather Data Query Agent with Memory")
    st.markdown("""
    This AI agent can:
    - Remember previous queries in your session
    - Understand follow-up questions in context
    - Provide continuous analysis without losing history
    - Suggest relevant follow-up questions
    """)
    
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    if 'current_query' not in st.session_state:
        st.session_state.current_query = None
    if 'follow_up_active' not in st.session_state:
        st.session_state.follow_up_active = False
    if 'follow_up_context' not in st.session_state:
        st.session_state.follow_up_context = ""
    
    
    if st.session_state.query_history:
        st.sidebar.title("Query History")
        for i, query in enumerate(st.session_state.query_history):
            if st.sidebar.button(f"Query {i+1}: {query['question'][:50]}...", key=f"hist_{i}"):
                st.session_state.current_query = i
        
        if st.session_state.current_query is not None:
            display_query_result(st.session_state.query_history[st.session_state.current_query])
    
    
    with st.form("main_query_form"):
        question = st.text_area("Ask your weather data question (English or Swahili):", 
                              height=100,
                              key="main_question")
        
        submitted = st.form_submit_button("Analyze with AI Agent", type="primary")
        
        if submitted and question:
            agent = SQLAgent()
            db_manager = DatabaseManager()
            
            with st.status("Processing your question...", expanded=True) as status:
                try:
                    
                    st.write("🔍 Analyzing your question with context...")
                    analysis = agent.analyze_question(question)
                    language = analysis.get("language", "english")
                    
                    
                    st.write("📝 Creating execution plan...")
                    plan = agent.generate_sql_plan(analysis, question)
                    
                   
                    st.write("💻 Generating SQL query...")
                    sql = agent.generate_initial_sql(plan, question, analysis)
                    
                    
                    valid, reason = agent.validate_sql(sql)
                    while not valid and agent.self_correction_attempts < agent.max_self_correction_attempts:
                        st.warning(f"⚠️ Found issue: {reason}")
                        st.write("🔄 Attempting self-correction...")
                        sql = agent.self_correct_sql(sql, reason, question)
                        if sql:
                            valid, reason = agent.validate_sql(sql)
                        else:
                            break
                    
                    if not valid:
                        st.error("❌ Could not generate valid SQL after multiple attempts.")
                        st.code(reason, language="text")
                        status.update(label="Processing failed", state="error")
                        return
                    
                    
                    st.write("⚡ Executing query...")
                    colnames, rows = db_manager.execute_query(sql)
                    results = [dict(zip(colnames, row)) for row in rows] if colnames else []
                    
                
                    st.write("📖 Generating explanations...")
                    explanation = agent.explain_query(sql, question, language, analysis)
                    
                    if results:
                        visualization_rec = agent.generate_visualization_recommendation(sql, results)
                        follow_ups = agent.generate_follow_up_questions(question, sql, results)
                    else:
                        visualization_rec = "No results to visualize."
                        follow_ups = []
                    
                   
                    agent.add_to_memory(question, sql, results)
                    
                   
                    query_data = {
                        "question": question,
                        "sql": sql,
                        "explanation": explanation,
                        "results": results,
                        "visualization_rec": visualization_rec,
                        "follow_ups": follow_ups,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "thinking_steps": agent.thinking_steps
                    }
                    
                    st.session_state.query_history.append(query_data)
                    st.session_state.current_query = len(st.session_state.query_history) - 1
                    
                    status.update(label="Processing complete!", state="complete")
                
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    st.code(traceback.format_exc(), language="python")
                    status.update(label="Processing failed", state="error")
                    return
            
            st.rerun()
    
  
    if st.session_state.query_history and st.session_state.current_query is not None:
        st.divider()
        st.subheader("🔍 Ask a Follow-up Question")
        
        current_context = st.session_state.query_history[st.session_state.current_query]
        
        follow_up_key = f"follow_up_{st.session_state.current_query}"
        follow_up = st.text_area("Ask a question related to the previous analysis:", 
                               height=100,
                               key=follow_up_key)
        
        if st.button("Submit Follow-up", key=f"follow_up_btn_{st.session_state.current_query}"):
            if follow_up:
                
                st.session_state.follow_up_context = f"In relation to my previous question '{current_context['question']}', {follow_up}"
                st.session_state.follow_up_active = True
                st.rerun()
    

    if st.session_state.get('follow_up_active', False) and st.session_state.follow_up_context:
       
        st.session_state.follow_up_active = False
        
        
        question = st.session_state.follow_up_context
       
        st.session_state.follow_up_context = ""
        
        agent = SQLAgent()
        db_manager = DatabaseManager()
        
        with st.status("Processing follow-up question...", expanded=True) as status:
            try:
            
                st.write("🔍 Analyzing follow-up question with context...")
                analysis = agent.analyze_question(question)
                language = analysis.get("language", "english")
                
               
                st.write("📝 Creating execution plan...")
                plan = agent.generate_sql_plan(analysis, question)
                
               
                st.write("💻 Generating SQL query...")
                sql = agent.generate_initial_sql(plan, question, analysis)
                
                
                valid, reason = agent.validate_sql(sql)
                while not valid and agent.self_correction_attempts < agent.max_self_correction_attempts:
                    st.warning(f"⚠️ Found issue: {reason}")
                    st.write("🔄 Attempting self-correction...")
                    sql = agent.self_correct_sql(sql, reason, question)
                    if sql:
                        valid, reason = agent.validate_sql(sql)
                    else:
                        break
                
                if not valid:
                    st.error("❌ Could not generate valid SQL after multiple attempts.")
                    st.code(reason, language="text")
                    status.update(label="Processing failed", state="error")
                    return
                
           
                st.write("⚡ Executing query...")
                colnames, rows = db_manager.execute_query(sql)
                results = [dict(zip(colnames, row)) for row in rows] if colnames else []
                
                st.write("📖 Generating explanations...")
                explanation = agent.explain_query(sql, question, language, analysis)
                
                if results:
                    visualization_rec = agent.generate_visualization_recommendation(sql, results)
                    follow_ups = agent.generate_follow_up_questions(question, sql, results)
                else:
                    visualization_rec = "No results to visualize."
                    follow_ups = []
                
                agent.add_to_memory(question, sql, results)
                
           
                query_data = {
                    "question": question,
                    "sql": sql,
                    "explanation": explanation,
                    "results": results,
                    "visualization_rec": visualization_rec,
                    "follow_ups": follow_ups,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "thinking_steps": agent.thinking_steps
                }
                
                st.session_state.query_history.append(query_data)
                st.session_state.current_query = len(st.session_state.query_history) - 1
                
                status.update(label="Follow-up processing complete!", state="complete")
            
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.code(traceback.format_exc(), language="python")
                status.update(label="Processing failed", state="error")
                return
        
     
        st.rerun()

if __name__ == "__main__":
    main()
    
    
    
    