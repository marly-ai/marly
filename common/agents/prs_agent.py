from typing import Annotated, Sequence, TypedDict, Literal
import operator
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
import functools
import logging
from .agent_enums import AgentMode, ExtractionPrompts, PageFinderPrompts

load_dotenv()

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]
    sender: str
    confidence_score: float
    reflections: list[str]
    iterations: int
    improvements: list[str]
    pending_fixes: list[str] 

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
    reflections = "\n".join(state.get("reflections", []))
    improvements = state.get("improvements", [])
    pending_fixes = state.get("pending_fixes", [])
    
    result = agent({
        "messages": state["messages"],
        "reflections": reflections,
        "improvements": improvements,
        "pending_fixes": pending_fixes
    })
    
    return {
        "messages": [AIMessage(content=result)],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "reflections": state["reflections"],
        "improvements": improvements,
        "pending_fixes": pending_fixes,
        "iterations": state["iterations"] + 1
    }

def analyze_node(state, agent, name, prompts):
    """Combined reflection and analysis node that evaluates output and generates specific solutions."""
    messages = state["messages"]
    last_message = messages[-1].content
    
    analysis_messages = [
        {"role": "system", "content": """You are a precise analysis expert. Your task is to:
        1. Evaluate the current extraction
        2. Identify specific issues and improvements
        3. Generate detailed, actionable solutions
        
        For each issue found:
        - Describe the exact problem
        - Provide specific solution steps
        - Note any dependencies or considerations
        - Explain how to verify the fix
        
        For each improvement identified:
        - Describe what aspect improved
        - Verify the improvement is correct
        - Note what should be maintained
        
        Focus on:
        - Consolidation of duplicate information
        - Accuracy of extracted data
        - Format consistency
        - Completeness of required information
        
        Remember: Be specific and actionable in your analysis."""},
        {"role": "user", "content": f"""Current extraction to analyze:
        {last_message}
        
        Previous improvements made:
        {chr(10).join(state.get('improvements', []))}
        
        Analyze this extraction and provide detailed solutions for any issues."""}
    ]
    
    # Use low temperature for precise analysis
    analysis = agent.do_completion(analysis_messages, temperature=0.2)
    
    # Parse the analysis into structured format
    solution_messages = [
        {"role": "system", "content": """Parse the analysis to extract:
        1. List of specific issues with their solutions
        2. List of verified improvements
        
        Format as JSON with two lists:
        {
            "issues": [{"issue": "description", "solution": "detailed steps"}],
            "improvements": ["specific improvement"]
        }"""},
        {"role": "user", "content": f"Analysis to parse:\n{analysis}"}
    ]
    
    parsed_result = agent.do_completion(solution_messages, temperature=0.0)
    
    try:
        import json
        parsed = json.loads(parsed_result)
        issues = [f"⚠ {item['issue']}\nSolution: {item['solution']}" for item in parsed["issues"]]
        improvements = [f"✓ {item}" for item in parsed["improvements"]]
    except:
        issues = ["⚠ Analysis parsing failed - needs review"]
        improvements = []
    
    current_improvements = state.get("improvements", [])
    current_pending_fixes = state.get("pending_fixes", [])
    
    # Update improvements and pending fixes
    for improvement in improvements:
        if improvement not in current_improvements:
            current_improvements.append(improvement)
    
    current_pending_fixes = [fix for fix in current_pending_fixes if not any(
        improvement.lower().replace("✓", "").strip() in fix.lower() 
        for improvement in improvements
    )]
    for issue in issues:
        if issue not in current_pending_fixes:
            current_pending_fixes.append(issue)
    
    # Store full analysis for context
    current_reflections = state.get("reflections", [])
    current_reflections.append(f"""Analysis {len(current_reflections) + 1}:
    {analysis}
    
    Issues Identified:
    {chr(10).join(issues) if issues else '(No new issues found)'}
    
    Improvements Verified:
    {chr(10).join(improvements) if improvements else '(No new improvements found)'}""")
    
    return {
        "messages": state["messages"],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "reflections": current_reflections,
        "improvements": current_improvements,
        "pending_fixes": current_pending_fixes,
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
        "reflections": state["reflections"],
        "iterations": state["iterations"]
    }

def fix_node(state, agent, name, prompts):
    """Fix identified issues using reflection details and pending fixes."""
    messages = state["messages"]
    last_message = messages[-1].content
    pending_fixes = state.get("pending_fixes", [])
    reflections = state.get("reflections", [])
    
    if not pending_fixes:
        return {
            "messages": state["messages"],
            "sender": name,
            "confidence_score": state["confidence_score"],
            "reflections": reflections,
            "improvements": state.get("improvements", []),
            "pending_fixes": [],
            "iterations": state["iterations"]
        }
    
    fix_messages = [
        {"role": "system", "content": """You are a precise issue-fixing assistant. Your task is to:
        1. Review the current extraction result
        2. Address each identified issue one by one
        3. Apply fixes while maintaining previous improvements
        
        Guidelines:
        - Focus on fixing ONLY the identified issues
        - Ensure each fix is properly applied
        - Maintain all previous improvements
        - Keep original formatting and units
        - Verify each fix before moving to the next
        
        Remember: Each issue must be explicitly addressed and fixed."""},
        {"role": "user", "content": f"""Current extraction:
        {last_message}
        
        Issues to fix:
        {chr(10).join(pending_fixes)}
        
        Previous reflections:
        {chr(10).join(reflections)}
        
        Fix each issue while maintaining the quality of working elements."""}
    ]
    
    # Use lower temperature for precise fixes
    fixed_result = agent.do_completion(fix_messages, temperature=0.2)
    
    return {
        "messages": [AIMessage(content=fixed_result)],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "reflections": reflections,
        "improvements": state.get("improvements", []),
        "pending_fixes": [],  # Clear pending fixes after applying them
        "iterations": state["iterations"]
    }

def create_router(mode: AgentMode):
    """Create a router function based on the agent mode."""
    def router(state) -> Literal["process", "analyze", "fix", "score", "synthesize", "__end__"]:
        iterations = state["iterations"]
        confidence = state["confidence_score"]
        pending_fixes = state.get("pending_fixes", [])
        
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
    all_responses = [msg.content for msg in state["messages"] if isinstance(msg, AIMessage)]
    improvements = state.get("improvements", [])
    
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
        "reflections": state["reflections"],
        "improvements": state["improvements"],
        "pending_fixes": state["pending_fixes"],
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

    graph = create_graph(client, mode)
    
    result = graph.invoke({
        "messages": [HumanMessage(content=text)],
        "sender": "user",
        "confidence_score": 0.0,
        "reflections": [],
        "improvements": [],
        "pending_fixes": [],
        "iterations": 0
    })
    
    logger.info("\n" + "="*50)
    logger.info(f"AGENT MODE: {mode.value}")
    logger.info("="*50)
    logger.info("\nFinal Result:")
    logger.info(result["messages"][-1].content)
    logger.info("\nConfidence Score: %.2f", result["confidence_score"])
    
    logger.info("\nImprovements Made:")
    for improvement in result.get("improvements", []):
        logger.info(improvement)
    
    logger.info("\nRemaining Issues:")
    pending = result.get("pending_fixes", [])
    if pending:
        for issue in pending:
            logger.info(issue)
    else:
        logger.info("(No remaining issues)")
    
    logger.info("\nDetailed Process Notes:")
    for reflection in result["reflections"]:
        logger.info(f"\n{reflection}")
    
    logger.info(f"\nTotal Iterations: {result['iterations']}")
    logger.info("="*50 + "\n")
    
    return result["messages"][-1].content