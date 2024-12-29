from typing import Annotated, Sequence, TypedDict, Literal
import operator
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
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

def agent_node(state, agent, name):
    """Process the agent's response and update the state."""
    session_id = state["session_id"]
    
    # Get state from Redis
    reflections = get_list(session_id, "reflections")
    improvements = get_list(session_id, "improvements")
    pending_fixes = get_list(session_id, "pending_fixes")
    
    result = agent({
        "messages": state["messages"],
        "reflections": "\n".join(reflections),
        "improvements": improvements,
        "pending_fixes": pending_fixes
    })
    
    return {
        "messages": [AIMessage(content=result)],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "session_id": session_id,
        "iterations": state["iterations"] + 1
    }

def analyze_node(state, agent, name, prompts):
    """Analyze output and identify consolidation opportunities."""
    messages = state["messages"]
    last_message = messages[-1].content
    session_id = state["session_id"]
    
    # Get previous improvements from Redis
    prev_improvements = get_list(session_id, "improvements")
    
    analysis_messages = [
        {"role": "system", "content": prompts.ANALYSIS.value},
        {"role": "user", "content": f"""Current extraction to analyze:
        {last_message}
        
        Previous improvements made:
        {chr(10).join(prev_improvements)}
        
        Analyze this extraction focusing on duplicate entries and consolidation opportunities."""}
    ]
    
    analysis = agent.do_completion(analysis_messages, temperature=0.2)
    
    # Extract issues and improvements using exact markers
    issues = []
    improvements = []
    
    for line in analysis.split('\n'):
        line = line.strip()
        if line.startswith('⚠'):
            issues.append(line)
        elif line.startswith('✓'):
            improvements.append(line)
    
    # Get current state from Redis
    current_improvements = get_list(session_id, "improvements")
    current_pending_fixes = get_list(session_id, "pending_fixes")
    
    # Add new improvements if not already present
    for improvement in improvements:
        if improvement not in current_improvements:
            current_improvements.append(improvement)
    
    # Remove fixed issues and add new ones
    current_pending_fixes = [fix for fix in current_pending_fixes if not any(
        improvement.replace('✓', '').strip() in fix.replace('⚠', '').strip()
        for improvement in improvements
    )]
    for issue in issues:
        if issue not in current_pending_fixes:
            current_pending_fixes.append(issue)
    
    # Store updated lists in Redis
    store_list(session_id, "improvements", current_improvements)
    store_list(session_id, "pending_fixes", current_pending_fixes)
    
    # Store analysis reflection in Redis
    reflection = f"""Analysis {len(get_list(session_id, 'reflections')) + 1}:
    
    Issues Found:
    {chr(10).join(issues) if issues else '(No issues found)'}
    
    Improvements Made:
    {chr(10).join(improvements) if improvements else '(No improvements made)'}"""
    
    redis_client.rpush(get_redis_key(session_id, "reflections"), json.dumps(reflection))
    redis_client.expire(get_redis_key(session_id, "reflections"), REDIS_EXPIRE)
    
    return {
        "messages": state["messages"],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "session_id": session_id,
        "iterations": state["iterations"]
    }

def confidence_node(state, agent, name, prompts):
    """Score the confidence of the current analysis."""
    messages = state["messages"]
    last_message = messages[-1].content
    
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
        "messages": state["messages"],
        "sender": name,
        "confidence_score": confidence,
        "session_id": state["session_id"],
        "iterations": state["iterations"]
    }

def fix_node(state, agent, name, prompts):
    """Fix identified issues focusing on deduplication."""
    messages = state["messages"]
    last_message = messages[-1].content
    session_id = state["session_id"]
    
    # Get state from Redis
    pending_fixes = get_list(session_id, "pending_fixes")
    reflections = get_list(session_id, "reflections", -1)  # Get only last reflection
    
    if not pending_fixes:
        return state
    
    fix_messages = [
        {"role": "system", "content": prompts.FIX.value},
        {"role": "user", "content": f"""Current extraction:
        {last_message}
        
        Issues to fix:
        {chr(10).join(pending_fixes)}
        
        Previous context:
        {chr(10).join(reflections)}
        
        Fix these issues, focusing on consolidation and deduplication."""}
    ]
    
    fixed_result = agent.do_completion(fix_messages, temperature=0.2)
    
    # Clear pending fixes in Redis
    redis_client.delete(get_redis_key(session_id, "pending_fixes"))
    
    return {
        "messages": [AIMessage(content=fixed_result)],
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
        session_id = state["session_id"]
        
        # Get pending fixes from Redis
        pending_fixes = get_list(session_id, "pending_fixes")
        
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
    all_responses = [msg.content for msg in state["messages"] if isinstance(msg, AIMessage)]
    improvements = get_list(session_id, "improvements")
    
    synthesis_messages = [
        {"role": "system", "content": prompts.SYNTHESIS.value},
        {"role": "user", "content": f"""Previous attempts:
        {chr(10).join([f'Attempt {i+1}:{chr(10)}{response}' for i, response in enumerate(all_responses)])}

        Verified Improvements:
        {chr(10).join(improvements)}

        Create the best possible final answer by combining the most accurate elements from all attempts."""}
    ]
    
    final_result = agent.do_completion(synthesis_messages, temperature=0.0)
    
    return {
        "messages": [AIMessage(content=final_result)],
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
    
    # Create unique session ID
    session_id = str(uuid.uuid4())
    
    graph = create_graph(client, mode)
    
    try:
        result = graph.invoke({
            "messages": [HumanMessage(content=text)],
            "sender": "user",
            "confidence_score": 0.0,
            "session_id": session_id,
            "iterations": 0
        })
        
        # Get final state from Redis for logging
        improvements = get_list(session_id, "improvements")
        pending_fixes = get_list(session_id, "pending_fixes")
        reflections = get_list(session_id, "reflections")
        
        logger.info("\n" + "="*50)
        logger.info(f"AGENT MODE: {mode.value}")
        logger.info("="*50)
        logger.info("\nFinal Result:")
        logger.info(result["messages"][-1].content)
        logger.info("\nConfidence Score: %.2f", result["confidence_score"])
        
        logger.info("\nImprovements Made:")
        for improvement in improvements:
            logger.info(improvement)
        
        logger.info("\nRemaining Issues:")
        if pending_fixes:
            for issue in pending_fixes:
                logger.info(issue)
        else:
            logger.info("(No remaining issues)")
        
        logger.info("\nDetailed Process Notes:")
        for reflection in reflections:
            logger.info(f"\n{reflection}")
        
        logger.info(f"\nTotal Iterations: {result['iterations']}")
        logger.info("="*50 + "\n")
        
        # Cleanup Redis keys
        for key_type in ["improvements", "pending_fixes", "reflections"]:
            redis_client.delete(get_redis_key(session_id, key_type))
        
        return result["messages"][-1].content
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        # Cleanup Redis keys on error
        for key_type in ["improvements", "pending_fixes", "reflections"]:
            redis_client.delete(get_redis_key(session_id, key_type))
        return "Extraction failed. Please try again."