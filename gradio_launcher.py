import gradio as gr
from src.gradio_app import create_gradio_interface

if __name__ == "__main__":
    interface = create_gradio_interface()
    interface.launch(
        server_name="0.0.0.0",
        server_port=7861,
        share=False
    )