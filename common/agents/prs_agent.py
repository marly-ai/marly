from typing import Annotated, Sequence, TypedDict, Literal
import operator
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
import functools
import logging
from .agent_enums import AgentMode, ExtractionPrompts, PageFinderPrompts
import json

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
    
    # Analysis focused on deduplication and consolidation
    analysis_messages = [
        {"role": "system", "content": prompts.ANALYSIS.value},
        {"role": "user", "content": f"""Current extraction to analyze:
        {last_message}
        
        Previous improvements made:
        {chr(10).join(state.get('improvements', []))}
        
        Analyze this extraction focusing on duplicate entries and consolidation opportunities."""}
    ]
    
    # Use low temperature for precise analysis
    analysis = agent.do_completion(analysis_messages, temperature=0.2)
    
    # Structure the analysis with focus on consolidation
    structure_messages = [
        {"role": "system", "content": """Structure the analysis into a detailed JSON format:
        {
            "metrics": {
                "duplicate_count": <number>,
                "consolidation_opportunities": <number>
            },
            "duplicates": [
                {
                    "type": "duplicate|conflicting|redundant",
                    "location": "<field_name>",
                    "entries": ["<value1>", "<value2>"],
                    "preferred_value": "<value>",
                    "reason": "<why_this_value>"
                }
            ],
            "consolidation_steps": [
                {
                    "field": "<field_name>",
                    "action": "merge|select|combine",
                    "values": ["<value1>", "<value2>"],
                    "result": "<consolidated_value>",
                    "verification": "<how_to_verify>"
                }
            ]
        }"""},
        {"role": "user", "content": f"Analysis to structure:\n{analysis}"}
    ]
    
    structured_result = agent.do_completion(structure_messages, temperature=0.0)
    
    try:
        import json
        analysis_details = json.loads(structured_result)
        
        # Generate human-readable issues focused on consolidation
        issues = [
            f"⚠ Duplicate in {dup['location']}: {', '.join(dup['entries'])}\n"
            f"Preferred: {dup['preferred_value']}\n"
            f"Reason: {dup['reason']}"
            for dup in analysis_details.get('duplicates', [])
        ]
        
        improvements = [
            f"✓ Consolidated {step['field']}: {step['result']}"
            for step in analysis_details.get('consolidation_steps', [])
            if step['action'] == 'merge'
        ]
    except Exception as e:
        issues = [f"⚠ Analysis structuring failed: {str(e)}"]
        improvements = []
        analysis_details = {"metrics": {"duplicate_count": 0, "consolidation_opportunities": 0}}
    
    # Update state with consolidation information
    current_improvements = state.get("improvements", [])
    current_pending_fixes = state.get("pending_fixes", [])
    
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
    
    # Store analysis details with focus on consolidation
    current_reflections = state.get("reflections", [])
    current_reflections.append(f"""Consolidation Analysis {len(current_reflections) + 1}:
    {analysis}
    
    Duplicates Found:
    - Count: {analysis_details['metrics']['duplicate_count']}
    - Consolidation Opportunities: {analysis_details['metrics']['consolidation_opportunities']}
    
    Issues Identified:
    {chr(10).join(issues) if issues else '(No duplicates found)'}
    
    Consolidations Completed:
    {chr(10).join(improvements) if improvements else '(No consolidations performed)'}""")
    
    return {
        "messages": state["messages"],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "reflections": current_reflections,
        "improvements": current_improvements,
        "pending_fixes": current_pending_fixes,
        "iterations": state["iterations"],
        "analysis_details": analysis_details
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
    """Fix identified issues with focus on deduplication and consolidation."""
    messages = state["messages"]
    last_message = messages[-1].content
    pending_fixes = state.get("pending_fixes", [])
    reflections = state.get("reflections", [])
    analysis_details = state.get("analysis_details", {})
    
    if not pending_fixes:
        return state
    
    # Create consolidation instructions
    consolidation_steps = []
    if analysis_details and 'consolidation_steps' in analysis_details:
        for step in analysis_details['consolidation_steps']:
            consolidation_steps.append(
                f"Consolidate {step['field']}:\n"
                f"Values: {', '.join(step['values'])}\n"
                f"Into: {step['result']}\n"
                f"Verify by: {step['verification']}"
            )
    
    fix_messages = [
        {"role": "system", "content": prompts.FIX.value},
        {"role": "user", "content": f"""Current extraction:
        {last_message}
        
        Consolidation Steps:
        {chr(10).join(consolidation_steps) if consolidation_steps else chr(10).join(pending_fixes)}
        
        Previous context:
        {chr(10).join(reflections)}
        
        Apply consolidation steps and verify each change."""}
    ]
    
    # Use low temperature for precise fixes
    fixed_result = agent.do_completion(fix_messages, temperature=0.2)
    
    # Verify consolidation results
    verify_messages = [
        {"role": "system", "content": prompts.VERIFICATION.value},
        {"role": "user", "content": f"""Original content:
        {last_message}
        
        Fixed content:
        {fixed_result}
        
        Verify all consolidations were applied correctly."""}
    ]
    
    verification_result = agent.do_completion(verify_messages, temperature=0.0)
    
    try:
        verification = json.loads(verification_result)
        consolidation_success = sum(1 for check in verification['consolidation_checks'] if check['success'])
        total_checks = len(verification['consolidation_checks'])
        success_rate = consolidation_success / total_checks if total_checks > 0 else 0.0
    except:
        verification = {
            "consolidation_checks": [],
            "remaining_duplicates": [{"type": "verification_failed", "reason": "Could not parse verification"}],
            "verification_score": 0.0
        }
        success_rate = 0.0
    
    return {
        "messages": [AIMessage(content=fixed_result)],
        "sender": name,
        "confidence_score": max(state["confidence_score"], success_rate),
        "reflections": reflections,
        "improvements": state.get("improvements", []),
        "pending_fixes": [f"⚠ Remaining duplicate: {dup['description']}" for dup in verification.get('remaining_duplicates', [])],
        "iterations": state["iterations"],
        "analysis_details": {
            **analysis_details,
            "consolidation_verification": verification
        }
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