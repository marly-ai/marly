from typing import Annotated, Sequence, TypedDict, Literal
import operator
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, AIMessage, HumanMessage
from langgraph.graph import StateGraph, END
import functools
import logging
import redis
import json
import uuid
from .agent_enums import AgentMode, ExtractionPrompts, PageFinderPrompts

load_dotenv()

# Redis connection
redis_client = redis.Redis(host='redis', port=6379, db=0)
REDIS_EXPIRE = 60 * 60  # 1 hour expiry

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]
    sender: str
    confidence_score: float
    session_id: str  # Added to track Redis keys
    iterations: int

def get_redis_key(session_id: str, key_type: str) -> str:
    """Generate Redis key for different state types."""
    return f"prs:{session_id}:{key_type}"

def store_list(session_id: str, key_type: str, items: list) -> None:
    """Store a list in Redis with expiry."""
    key = get_redis_key(session_id, key_type)
    if items:
        redis_client.delete(key)
        redis_client.rpush(key, *[json.dumps(item) for item in items])
        redis_client.expire(key, REDIS_EXPIRE)

def get_list(session_id: str, key_type: str, start: int = 0, end: int = -1) -> list:
    """Get a list from Redis with optional range."""
    key = get_redis_key(session_id, key_type)
    items = redis_client.lrange(key, start, end)
    return [json.loads(item) for item in items] if items else []

def get_prompts(mode: AgentMode):
    """Get the appropriate prompts for the specified mode."""
    if mode == AgentMode.EXTRACTION:
        return ExtractionPrompts
    return PageFinderPrompts

def create_agent(client, system_message: str):
    """Create an agent with a specific system message."""
    def agent_fn(inputs):
        improvements_made = "\n".join(inputs.get("improvements", []))
        pending_fixes = "\n".join(inputs.get("pending_fixes", []))
        
        messages = [
            {"role": "system", "content": system_message},
            {"role": "system", "content": f"""Previous reflections identified these issues to fix:
        {pending_fixes}

        Improvements already made:
        {improvements_made}

        Focus on addressing the pending issues while maintaining previous improvements."""},
                    {"role": "user", "content": inputs["messages"][-1].content}
        ]
        return client.do_completion(messages)
    return agent_fn

def store_message(session_id: str, content: str, role: str = "ai") -> None:
    """Store a message in Redis."""
    key = get_redis_key(session_id, "messages")
    redis_client.rpush(key, json.dumps({"role": role, "content": content}))
    redis_client.expire(key, REDIS_EXPIRE)

def get_last_message(session_id: str) -> str:
    """Get the last message content from Redis."""
    key = get_redis_key(session_id, "messages")
    last_msg = redis_client.lindex(key, -1)
    if last_msg:
        return json.loads(last_msg)["content"]
    return ""

def get_all_messages(session_id: str) -> list:
    """Get all messages from Redis."""
    key = get_redis_key(session_id, "messages")
    messages = redis_client.lrange(key, 0, -1)
    return [json.loads(msg) for msg in messages] if messages else []

def agent_node(state, agent, name):
    """Process the agent's response and update the state."""
    session_id = state["session_id"]
    messages = state["messages"]  # Keep original messages
    
    # Get only necessary context
    improvements = get_list(session_id, "improvements", -5)
    pending_fixes = get_list(session_id, "pending_fixes")
    
    result = agent({
        "messages": messages,  # Pass full messages object
        "improvements": improvements,
        "pending_fixes": pending_fixes
    })
    
    # Store result in Redis
    store_message(session_id, result)
    
    return {
        "messages": messages + [AIMessage(content=result)],  # Keep message chain
        "sender": name,
        "confidence_score": state["confidence_score"],
        "session_id": session_id,
        "iterations": state["iterations"] + 1
    }

def analyze_node(state, agent, name, prompts):
    """Analyze output and identify consolidation opportunities."""
    session_id = state["session_id"]
    last_message = get_last_message(session_id)
    
    # Get full context for better analysis
    prev_improvements = get_list(session_id, "improvements")
    
    # Initial deep analysis
    analysis_messages = [
        {"role": "system", "content": prompts.ANALYSIS.value},
        {"role": "user", "content": f"""Current extraction to analyze:
        {last_message}
        
        Previous improvements made:
        {chr(10).join(prev_improvements)}
        
        Analyze this extraction focusing on duplicate entries and consolidation opportunities.
        Be thorough and identify ALL potential duplicates and conflicts."""}
    ]
    
    analysis = agent.do_completion(analysis_messages, temperature=0.2)
    
    # Secondary verification analysis
    verification_messages = [
        {"role": "system", "content": """Verify the previous analysis for:
        1. Missed duplicates or conflicts
        2. False positives in identified issues
        3. Completeness of consolidation opportunities
        4. Accuracy of proposed improvements"""},
        {"role": "user", "content": f"""Previous analysis:
        {analysis}
        
        Original content:
        {last_message}
        
        Verify and identify any missed issues or inaccuracies."""}
    ]
    
    verification = agent.do_completion(verification_messages, temperature=0.1)
    
    # Process both analyses
    current_improvements = get_list(session_id, "improvements")
    current_pending_fixes = get_list(session_id, "pending_fixes")
    
    new_improvements = []
    new_fixes = []
    
    # Process both analysis and verification
    for content in [analysis, verification]:
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue
            if line.startswith('✓'):
                if line not in current_improvements and line not in new_improvements:
                    new_improvements.append(line)
            elif line.startswith('⚠'):
                if line not in current_pending_fixes and line not in new_fixes:
                    new_fixes.append(line)
    
    # Batch Redis updates
    if new_improvements:
        store_list(session_id, "improvements", current_improvements + new_improvements)
    if new_fixes:
        store_list(session_id, "pending_fixes", new_fixes)
    
    # Store detailed reflection
    reflection = f"""Analysis {redis_client.llen(get_redis_key(session_id, 'reflections')) + 1}:
    
    Initial Analysis:
    {analysis}
    
    Verification:
    {verification}
    
    New Issues: {len(new_fixes)}
    New Improvements: {len(new_improvements)}"""
    
    redis_client.rpush(get_redis_key(session_id, "reflections"), json.dumps(reflection))
    
    return {
        "messages": state["messages"],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "session_id": session_id,
        "iterations": state["iterations"]
    }

def confidence_node(state, agent, name, prompts):
    """Score the confidence of the current analysis."""
    session_id = state["session_id"]
    messages = state["messages"]
    last_message = messages[-1].content if messages else ""
    
    confidence_messages = [
        {"role": "system", "content": prompts.CONFIDENCE.value},
        {"role": "user", "content": f"Analysis to score:\n{last_message}"}
    ]
    
    score = agent.do_completion(confidence_messages, temperature=0.0)
    
    try:
        confidence = float(score.strip())
    except:
        confidence = 0.5
    
    return {
        "messages": messages,  # Keep original messages
        "sender": name,
        "confidence_score": confidence,
        "session_id": session_id,
        "iterations": state["iterations"]
    }

def fix_node(state, agent, name, prompts):
    """Fix identified issues focusing on deduplication."""
    session_id = state["session_id"]
    last_message = get_last_message(session_id)
    pending_fixes = get_list(session_id, "pending_fixes")
    
    if not pending_fixes:
        return state
    
    # Initial fix attempt
    fix_messages = [
        {"role": "system", "content": prompts.FIX.value},
        {"role": "user", "content": f"""Content to fix:
        {last_message}
        
        Issues to address:
        {chr(10).join(pending_fixes)}
        
        Apply fixes systematically and verify each change."""}
    ]
    
    fixed_result = agent.do_completion(fix_messages, temperature=0.2)
    
    # Verify fixes
    verify_messages = [
        {"role": "system", "content": """Verify that all fixes were properly applied:
        1. Check each issue was addressed
        2. Verify no information was lost
        3. Confirm all consolidations are accurate
        4. Ensure no new duplicates were created"""},
        {"role": "user", "content": f"""Original content:
        {last_message}
        
        Applied fixes:
        {fixed_result}
        
        Original issues:
        {chr(10).join(pending_fixes)}
        
        Verify all fixes were properly applied."""}
    ]
    
    verification = agent.do_completion(verify_messages, temperature=0.1)
    
    # Store both results and verification in Redis
    store_message(session_id, fixed_result)
    redis_client.rpush(get_redis_key(session_id, "fix_verifications"), 
                      json.dumps({"fixes": pending_fixes, "verification": verification}))
    
    # Clear pending fixes only after verification
    redis_client.delete(get_redis_key(session_id, "pending_fixes"))
    
    return {
        "messages": state["messages"] + [AIMessage(content=fixed_result)],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "session_id": session_id,
        "iterations": state["iterations"]
    }

def create_router(mode: AgentMode):
    """Create a router function based on the agent mode."""
    def router(state) -> Literal["process", "analyze", "fix", "score", "synthesize", "__end__"]:
        iterations = state["iterations"]
        confidence = state["confidence_score"]
        
        max_iterations = 2
        min_confidence = 0.8
        
        # Check if we've hit max iterations or have sufficient confidence
        if iterations >= max_iterations or confidence >= min_confidence:
            if state["sender"] != "synthesizer":
                return "synthesize"
            return "__end__"
        
        # Main flow: Process -> Analyze -> Fix -> Score -> [Loop or Synthesize]
        if state["sender"] == "user":
            return "process"
        elif state["sender"] == "processor":
            return "analyze"
        elif state["sender"] == "analyzer":
            return "fix"
        elif state["sender"] == "fixer":
            return "score"
        elif state["sender"] == "scorer":
            # If score is low and we haven't hit max iterations, loop back to process
            if confidence < min_confidence and iterations < max_iterations:
                return "process"
            return "synthesize"
        else:
            return "process"
    
    return router

def synthesize_node(state, agent, name, prompts):
    """Create final answer by synthesizing all iterations and improvements."""
    session_id = state["session_id"]
    
    # Get only the successful responses and recent improvements
    messages = get_all_messages(session_id)
    improvements = get_list(session_id, "improvements", -5)
    
    # Final LLM call with focused context
    final_result = agent.do_completion([
        {"role": "system", "content": prompts.SYNTHESIS.value},
        {"role": "user", "content": f"""Best response so far:
        {messages[-1]['content'] if messages else ''}
        
        Key improvements:
        {chr(10).join(improvements)}"""}
    ], temperature=0.0)
    
    # Store final result
    store_message(session_id, final_result)
    
    return {
        "sender": "synthesizer",
        "confidence_score": state["confidence_score"],
        "session_id": session_id,
        "iterations": state["iterations"]
    }

def create_graph(client, mode: AgentMode):
    """Create the workflow graph with the specified mode."""
    prompts = get_prompts(mode)
    
    text_processor = create_agent(client, prompts.SYSTEM.value)
    
    processor_node = functools.partial(agent_node, agent=text_processor, name="processor")
    analyzer = functools.partial(analyze_node, agent=client, name="analyzer", prompts=prompts)
    issue_fixer = functools.partial(fix_node, agent=client, name="fixer", prompts=prompts)
    confidence_scorer = functools.partial(confidence_node, agent=client, name="scorer", prompts=prompts)
    synthesizer = functools.partial(synthesize_node, agent=client, name="synthesizer", prompts=prompts)
    
    workflow = StateGraph(AgentState)
    
    workflow.add_node("process", processor_node)
    workflow.add_node("analyze", analyzer)
    workflow.add_node("fix", issue_fixer)
    workflow.add_node("score", confidence_scorer)
    workflow.add_node("synthesize", synthesizer)
    
    router = create_router(mode)
    
    # Add edges for all nodes
    for node in ["process", "analyze", "fix", "score", "synthesize"]:
        workflow.add_conditional_edges(
            node,
            router,
            {
                "process": "process",
                "analyze": "analyze",
                "fix": "fix",
                "score": "score",
                "synthesize": "synthesize",
                "__end__": END,
            },
        )
    
    workflow.set_entry_point("process")
    
    return workflow.compile()

def process_extraction(text: str, client, mode: AgentMode) -> str:
    """Process text through the agent workflow with extraction handler format."""
    logger = logging.getLogger(__name__)
    session_id = str(uuid.uuid4())
    
    try:
        # Store initial message
        store_message(session_id, text, "human")
        
        # Create graph before using it
        graph = create_graph(client, mode)
        
        # Execute graph with initial message
        result = graph.invoke({
            "messages": [HumanMessage(content=text)],  # Start with HumanMessage
            "sender": "user",
            "confidence_score": 0.0,
            "session_id": session_id,
            "iterations": 0
        })
        
        # Get final result
        final_message = result["messages"][-1].content if result["messages"] else ""
        
        # Cleanup all Redis keys
        for key_type in ["messages", "improvements", "pending_fixes", "reflections"]:
            redis_client.delete(get_redis_key(session_id, key_type))
        
        return final_message
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        # Cleanup Redis keys
        for key_type in ["messages", "improvements", "pending_fixes", "reflections"]:
            redis_client.delete(get_redis_key(session_id, key_type))
        return "Extraction failed. Please try again."