# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: wordcount_mr.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x12wordcount_mr.proto\x12\x0cwordcount_mr\"\xef\x03\n\x04Task\x12)\n\x04type\x18\x01 \x01(\x0e\x32\x1b.wordcount_mr.Task.TaskType\x12\x17\n\ninput_path\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x18\n\x0boutput_path\x18\x03 \x01(\tH\x01\x88\x01\x01\x12\x18\n\x10input_file_names\x18\x04 \x03(\t\x12\x1a\n\routput_prefix\x18\x05 \x01(\tH\x02\x88\x01\x01\x12\x13\n\x06job_id\x18\x06 \x01(\x05H\x03\x88\x01\x01\x12\x18\n\x0bignore_case\x18\x07 \x01(\x08H\x04\x88\x01\x01\x12\x11\n\x04sort\x18\x08 \x01(\x08H\x05\x88\x01\x01\x12\x16\n\tn_buckets\x18\t \x01(\x05H\x06\x88\x01\x01\x12\x17\n\nmerge_join\x18\n \x01(\x08H\x07\x88\x01\x01\x12\x1e\n\x11wait_milliseconds\x18\x0b \x01(\x05H\x08\x88\x01\x01\"8\n\x08TaskType\x12\x07\n\x03MAP\x10\x00\x12\n\n\x06REDUCE\x10\x01\x12\x08\n\x04WAIT\x10\x02\x12\r\n\tTERMINATE\x10\x03\x42\r\n\x0b_input_pathB\x0e\n\x0c_output_pathB\x10\n\x0e_output_prefixB\t\n\x07_job_idB\x0e\n\x0c_ignore_caseB\x07\n\x05_sortB\x0c\n\n_n_bucketsB\r\n\x0b_merge_joinB\x14\n\x12_wait_milliseconds\"\xad\x01\n\x0bTaskRequest\x12\x30\n\x06status\x18\x01 \x01(\x0e\x32 .wordcount_mr.TaskRequest.Status\x12\x11\n\tworker_id\x18\x02 \x01(\x05\x12\x19\n\x11output_file_names\x18\x03 \x03(\t\x12\x10\n\x08warnings\x18\x04 \x03(\t\",\n\x06Status\x12\x08\n\x04INIT\x10\x00\x12\x0b\n\x07SUCCESS\x10\x01\x12\x0b\n\x07\x46\x41ILURE\x10\x02\x32I\n\x0bWordCountMR\x12:\n\x07GetTask\x12\x19.wordcount_mr.TaskRequest\x1a\x12.wordcount_mr.Task\"\x00\x62\x06proto3')



_TASK = DESCRIPTOR.message_types_by_name['Task']
_TASKREQUEST = DESCRIPTOR.message_types_by_name['TaskRequest']
_TASK_TASKTYPE = _TASK.enum_types_by_name['TaskType']
_TASKREQUEST_STATUS = _TASKREQUEST.enum_types_by_name['Status']
Task = _reflection.GeneratedProtocolMessageType('Task', (_message.Message,), {
  'DESCRIPTOR' : _TASK,
  '__module__' : 'wordcount_mr_pb2'
  # @@protoc_insertion_point(class_scope:wordcount_mr.Task)
  })
_sym_db.RegisterMessage(Task)

TaskRequest = _reflection.GeneratedProtocolMessageType('TaskRequest', (_message.Message,), {
  'DESCRIPTOR' : _TASKREQUEST,
  '__module__' : 'wordcount_mr_pb2'
  # @@protoc_insertion_point(class_scope:wordcount_mr.TaskRequest)
  })
_sym_db.RegisterMessage(TaskRequest)

_WORDCOUNTMR = DESCRIPTOR.services_by_name['WordCountMR']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _TASK._serialized_start=37
  _TASK._serialized_end=532
  _TASK_TASKTYPE._serialized_start=340
  _TASK_TASKTYPE._serialized_end=396
  _TASKREQUEST._serialized_start=535
  _TASKREQUEST._serialized_end=708
  _TASKREQUEST_STATUS._serialized_start=664
  _TASKREQUEST_STATUS._serialized_end=708
  _WORDCOUNTMR._serialized_start=710
  _WORDCOUNTMR._serialized_end=783
# @@protoc_insertion_point(module_scope)
