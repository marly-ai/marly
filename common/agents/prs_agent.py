from typing import Annotated, Sequence, TypedDict, Literal
import operator
from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
import functools
import logging

load_dotenv()

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]
    sender: str
    confidence_score: float
    reflections: list[str]
    iterations: int

def create_agent(client, system_message: str):
    """Create an agent with a specific system message."""
    def agent_fn(inputs):
        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": inputs["messages"][-1].content},
            {"role": "system", "content": f"Previous reflections:\n{inputs['reflections']}"}
        ]
        return client.do_completion(messages)
    return agent_fn

def agent_node(state, agent, name):
    """Process the agent's response and update the state."""
    reflections = "\n".join(state.get("reflections", []))
    result = agent({
        "messages": state["messages"],
        "reflections": reflections
    })
    return {
        "messages": [AIMessage(content=result)],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "reflections": state["reflections"],
        "iterations": state["iterations"] + 1
    }

def reflection_node(state, agent, name):
    """Generate reflections on the current response."""
    messages = state["messages"]
    last_message = messages[-1].content
    
    reflection_messages = [
        {"role": "system", "content": """Analyze the data extraction attempt with these critical points:
        - Completeness: Are ALL required metrics covered?
        - Format Adherence: Does the output exactly match the example format?
        - Data Accuracy: Are values identical to source?
        - Consistency: Are units and notations uniform?
        - Structure: Is the hierarchical/sequential format maintained?
        - Missing Data: Are unavailable data points properly marked?
        Keep reflection focused and actionable (2-3 sentences)."""},
        {"role": "user", "content": f"Response to reflect on:\n{last_message}"}
    ]
    
    reflection = agent.do_completion(reflection_messages)
    
    current_reflections = state.get("reflections", [])
    current_reflections.append(reflection)
    
    return {
        "messages": state["messages"],
        "sender": name,
        "confidence_score": state["confidence_score"],
        "reflections": current_reflections,
        "iterations": state["iterations"]
    }

def confidence_node(state, agent, name):
    """Score the confidence of the current analysis."""
    messages = state["messages"]
    last_message = messages[-1].content
    
    confidence_messages = [
        {"role": "system", "content": """Rate the extraction confidence on a scale of 0.0 to 1.0.
        Evaluate these critical factors:
        - Completeness: All required metrics extracted
        - Accuracy: Values match source exactly
        - Format: Strict adherence to example format
        - Consistency: Uniform handling of units/notations
        - Verification: Clear traceability to source text
        - Structure: Proper hierarchical/sequential organization
        Respond with ONLY a number between 0.0 and 1.0."""},
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

def router(state) -> Literal["process", "reflect", "score", "__end__"]:
    """Route to the next node or end."""
    messages = state["messages"]
    last_message = messages[-1].content
    iterations = state["iterations"]
    confidence = state["confidence_score"]
    
    if "FINAL ANSWER" in last_message and iterations >= 3 and confidence >= 0.8:
        return "__end__"
    
    if iterations >= 4:
        return "__end__"
    
    if state["sender"] == "user":
        return "process"
    elif state["sender"] == "processor":
        return "reflect"
    elif state["sender"] == "reflection":
        return "score"
    else:
        return "process"

def create_graph(client):
    """Create the workflow graph."""
    text_processor = create_agent(
        client,
        system_message="""You are a precise data extraction and structuring assistant. Your task is to:
        1. Extract all required data points from the source document
        2. Format the extracted data exactly according to the provided example format
        3. Ensure complete coverage of all metrics specified in the metrics_list
        
        Critical Guidelines:
        - Extract information ONLY if it exists in the source text
        - Match the example format's structure and style exactly
        - Maintain consistent formatting for all numerical values
        - Preserve original units and notations when present
        - Follow any hierarchical or sequential structure in the example
        - Mark truly missing data points as "Not Available"
        - Double-check all extracted values against source
        - When confident in extraction completeness and accuracy, start with 'FINAL ANSWER:'
        
        Consider previous reflections to improve precision and completeness.
        Accuracy and format consistency are paramount."""
    )
    
    processor_node = functools.partial(agent_node, agent=text_processor, name="processor")
    reflection_processor = functools.partial(reflection_node, agent=client, name="reflection")
    confidence_scorer = functools.partial(confidence_node, agent=client, name="confidence")
    
    workflow = StateGraph(AgentState)
    
    workflow.add_node("process", processor_node)
    workflow.add_node("reflect", reflection_processor)
    workflow.add_node("score", confidence_scorer)
    
    workflow.add_conditional_edges(
        "process",
        router,
        {
            "process": "process",
            "reflect": "reflect",
            "score": "score",
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
            "__end__": END,
        },
    )
    
    workflow.set_entry_point("process")
    
    return workflow.compile()

def process_extraction(formatted_content: str, keywords: str, examples: str, client) -> str:
    """Process text through the agent workflow with extraction handler format."""
    logger = logging.getLogger(__name__)
    
    text = f"""
    SOURCE DOCUMENT:
    {formatted_content}

    EXAMPLE FORMAT:
    {examples}

    METRICS TO EXTRACT:
    {keywords}
    """

    graph = create_graph(client)
    
    result = graph.invoke({
        "messages": [HumanMessage(content=text)],
        "sender": "user",
        "confidence_score": 0.0,
        "reflections": [],
        "iterations": 0
    })
    
    logger.info("EXTRACTION PROCESS DETAILS")
    logger.info("\nFinal Extraction:")
    logger.info(result["messages"][-1].content)
    logger.info("\nConfidence Score: %.2f", result["confidence_score"])
    logger.info("\nExtraction Process Notes:")
    for i, reflection in enumerate(result["reflections"], 1):
        logger.info(f"\n{i}. {reflection}")
    logger.info(f"\nTotal Iterations: {result['iterations']}")
    
    return result["messages"][-1].content