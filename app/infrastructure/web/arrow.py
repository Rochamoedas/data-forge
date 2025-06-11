from fastapi.responses import Response
import pyarrow as pa
import pyarrow.ipc as ipc

class ArrowResponse(Response):
    media_type = "application/vnd.apache.arrow.stream"

    def __init__(self, table: pa.Table, **kwargs):
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        
        content = sink.getvalue().to_pybytes()
        super().__init__(content=content, media_type=self.media_type, **kwargs) 