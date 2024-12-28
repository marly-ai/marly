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

def analyze_reflection(reflection: str) -> tuple[list[str], list[str]]:
    """Analyze a reflection to extract specific issues and improvements."""
    issues = []
    improvements = []
    
    if "complete" in reflection.lower() and "all metrics" in reflection.lower():
        improvements.append("✓ All required metrics are now covered")
    if "format matches" in reflection.lower() or "follows format" in reflection.lower():
        improvements.append("✓ Output format matches the example exactly")
    if "consistent" in reflection.lower() and "units" in reflection.lower():
        improvements.append("✓ Units and notations are consistent")
    
    if "missing" in reflection.lower():
        issues.append("⚠ Some metrics are missing or incomplete")
    if "format" in reflection.lower() and ("incorrect" in reflection.lower() or "wrong" in reflection.lower()):
        issues.append("⚠ Format needs adjustment")
    if "inconsistent" in reflection.lower():
        issues.append("⚠ Inconsistencies in units or notation")
    
    return issues, improvements

def reflection_node(state, agent, name, prompts):
    """Generate reflections and analyze them for improvements and issues."""
    messages = state["messages"]
    last_message = messages[-1].content
    
    reflection_messages = [
        {"role": "system", "content": prompts.REFLECTION.value},
        {"role": "user", "content": f"Response to reflect on:\n{last_message}"}
    ]
    
    reflection = agent.do_completion(reflection_messages)
    
    new_issues, new_improvements = analyze_reflection(reflection)
    
    current_reflections = state.get("reflections", [])
    current_improvements = state.get("improvements", [])
    current_pending_fixes = state.get("pending_fixes", [])
    
    for improvement in new_improvements:
        if improvement not in current_improvements:
            current_improvements.append(improvement)
    
    current_pending_fixes = [fix for fix in current_pending_fixes if not any(
        improvement.lower().replace("✓", "").strip() in fix.lower() 
        for improvement in new_improvements
    )]
    for issue in new_issues:
        if issue not in current_pending_fixes:
            current_pending_fixes.append(issue)
    
    current_reflections.append(f"""Reflection {len(current_reflections) + 1}:
    {reflection}

    Issues Identified:
    {chr(10).join(new_issues) if new_issues else '(No new issues found)'}

    Improvements Made:
    {chr(10).join(new_improvements) if new_improvements else '(No new improvements found)'}""")
    
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
    
    score = agent.do_completion(confidence_messages)
    
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

def create_router(mode: AgentMode):
    """Create a router function based on the agent mode."""
    def router(state) -> Literal["process", "reflect", "score", "synthesize", "__end__"]:
        messages = state["messages"]
        last_message = messages[-1].content
        iterations = state["iterations"]
        confidence = state["confidence_score"]
        
        min_iterations = 3 if mode == AgentMode.EXTRACTION else 2
        max_iterations = 4 if mode == AgentMode.EXTRACTION else 3
        min_confidence = 0.8
        
        # After minimum iterations, check if we should synthesize final answer
        if iterations >= min_iterations and (
            confidence >= min_confidence or iterations >= max_iterations
        ):
            if state["sender"] != "synthesizer":
                return "synthesize"
            return "__end__"
        
        if state["sender"] == "user":
            return "process"
        elif state["sender"] == "processor":
            return "reflect"
        elif state["sender"] == "reflection":
            return "score"
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
    
    final_result = agent.do_completion(synthesis_messages)
    
    return {
        "messages": [AIMessage(content=final_result)],
        "sender": "synthesizer",
        "confidence_score": state["confidence_score"],
        "reflections": state["reflections"],
        "improvements": state["improvements"],
        "pending_fixes": state["pending_fixes"],
        "iterations": state["iterations"]
    }

def create_graph(client, mode: AgentMode = AgentMode.EXTRACTION):
    """Create the workflow graph with the specified mode."""
    prompts = get_prompts(mode)
    
    text_processor = create_agent(client, prompts.SYSTEM.value)
    
    processor_node = functools.partial(agent_node, agent=text_processor, name="processor")
    reflection_processor = functools.partial(reflection_node, agent=client, name="reflection", prompts=prompts)
    confidence_scorer = functools.partial(confidence_node, agent=client, name="confidence", prompts=prompts)
    synthesizer = functools.partial(synthesize_node, agent=client, name="synthesizer", prompts=prompts)
    
    workflow = StateGraph(AgentState)
    
    workflow.add_node("process", processor_node)
    workflow.add_node("reflect", reflection_processor)
    workflow.add_node("score", confidence_scorer)
    workflow.add_node("synthesize", synthesizer)
    
    router = create_router(mode)
    
    workflow.add_conditional_edges(
        "process",
        router,
        {
            "process": "process",
            "reflect": "reflect",
            "score": "score",
            "synthesize": "synthesize",
            "__end__": END,
        },
    )
    
    workflow.add_conditional_edges(
        "reflect",
        router,
        {
            "process": "process",
            "reflect": "reflect",
            "score": "score",
            "synthesize": "synthesize",
            "__end__": END,
        },
    )
    
    workflow.add_conditional_edges(
        "score",
        router,
        {
            "process": "process",
            "reflect": "reflect",
            "score": "score",
            "synthesize": "synthesize",
            "__end__": END,
        },
    )
    
    workflow.add_conditional_edges(
        "synthesize",
        router,
        {
            "process": "process",
            "reflect": "reflect",
            "score": "score",
            "synthesize": "synthesize",
            "__end__": END,
        },
    )
    
    workflow.set_entry_point("process")
    
    return workflow.compile()

def process_extraction(formatted_content: str, keywords: str, examples: str, client, mode: AgentMode = AgentMode.EXTRACTION) -> str:
    """Process text through the agent workflow with extraction handler format."""
    logger = logging.getLogger(__name__)
    
    text = f"""SOURCE DOCUMENT:
            {formatted_content}

            EXAMPLE FORMAT:
            {examples}

            METRICS TO EXTRACT:
            {keywords}"""

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