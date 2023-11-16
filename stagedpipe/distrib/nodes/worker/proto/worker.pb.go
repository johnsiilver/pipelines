// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        (unknown)
// source: pipelines/stagedpipe/distrib/nodes/worker/proto/worker.proto

package workerproto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RequestGroupReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Message:
	//	*RequestGroupReq_OpenGroup
	//	*RequestGroupReq_Data
	Message isRequestGroupReq_Message `protobuf_oneof:"message"`
}

func (x *RequestGroupReq) Reset() {
	*x = RequestGroupReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestGroupReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestGroupReq) ProtoMessage() {}

func (x *RequestGroupReq) ProtoReflect() protoreflect.Message {
	mi := &file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestGroupReq.ProtoReflect.Descriptor instead.
func (*RequestGroupReq) Descriptor() ([]byte, []int) {
	return file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescGZIP(), []int{0}
}

func (m *RequestGroupReq) GetMessage() isRequestGroupReq_Message {
	if m != nil {
		return m.Message
	}
	return nil
}

func (x *RequestGroupReq) GetOpenGroup() *OpenGroupReq {
	if x, ok := x.GetMessage().(*RequestGroupReq_OpenGroup); ok {
		return x.OpenGroup
	}
	return nil
}

func (x *RequestGroupReq) GetData() []byte {
	if x, ok := x.GetMessage().(*RequestGroupReq_Data); ok {
		return x.Data
	}
	return nil
}

type isRequestGroupReq_Message interface {
	isRequestGroupReq_Message()
}

type RequestGroupReq_OpenGroup struct {
	OpenGroup *OpenGroupReq `protobuf:"bytes,1,opt,name=open_group,json=openGroup,proto3,oneof"`
}

type RequestGroupReq_Data struct {
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3,oneof"`
}

func (*RequestGroupReq_OpenGroup) isRequestGroupReq_Message() {}

func (*RequestGroupReq_Data) isRequestGroupReq_Message() {}

type OpenGroupReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The name of the plugin to open.
	Plugin string `protobuf:"bytes,1,opt,name=plugin,proto3" json:"plugin,omitempty"`
	// The JSON encoded RGConfig for this RequestGroup for this plugin.
	Config []byte `protobuf:"bytes,2,opt,name=config,proto3" json:"config,omitempty"`
}

func (x *OpenGroupReq) Reset() {
	*x = OpenGroupReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OpenGroupReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OpenGroupReq) ProtoMessage() {}

func (x *OpenGroupReq) ProtoReflect() protoreflect.Message {
	mi := &file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OpenGroupReq.ProtoReflect.Descriptor instead.
func (*OpenGroupReq) Descriptor() ([]byte, []int) {
	return file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescGZIP(), []int{1}
}

func (x *OpenGroupReq) GetPlugin() string {
	if x != nil {
		return x.Plugin
	}
	return ""
}

func (x *OpenGroupReq) GetConfig() []byte {
	if x != nil {
		return x.Config
	}
	return nil
}

type RequestGroupResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *RequestGroupResp) Reset() {
	*x = RequestGroupResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestGroupResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestGroupResp) ProtoMessage() {}

func (x *RequestGroupResp) ProtoReflect() protoreflect.Message {
	mi := &file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestGroupResp.ProtoReflect.Descriptor instead.
func (*RequestGroupResp) Descriptor() ([]byte, []int) {
	return file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescGZIP(), []int{2}
}

func (x *RequestGroupResp) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto protoreflect.FileDescriptor

var file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDesc = []byte{
	0x0a, 0x3c, 0x70, 0x69, 0x70, 0x65, 0x6c, 0x69, 0x6e, 0x65, 0x73, 0x2f, 0x73, 0x74, 0x61, 0x67,
	0x65, 0x64, 0x70, 0x69, 0x70, 0x65, 0x2f, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x2f, 0x6e,
	0x6f, 0x64, 0x65, 0x73, 0x2f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1f,
	0x73, 0x74, 0x61, 0x67, 0x65, 0x64, 0x70, 0x69, 0x70, 0x65, 0x2e, 0x64, 0x69, 0x73, 0x74, 0x72,
	0x69, 0x62, 0x2e, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x2e, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x22,
	0x82, 0x01, 0x0a, 0x0f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70,
	0x52, 0x65, 0x71, 0x12, 0x4e, 0x0a, 0x0a, 0x6f, 0x70, 0x65, 0x6e, 0x5f, 0x67, 0x72, 0x6f, 0x75,
	0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2d, 0x2e, 0x73, 0x74, 0x61, 0x67, 0x65, 0x64,
	0x70, 0x69, 0x70, 0x65, 0x2e, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x2e, 0x6e, 0x6f, 0x64,
	0x65, 0x73, 0x2e, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2e, 0x4f, 0x70, 0x65, 0x6e, 0x47, 0x72,
	0x6f, 0x75, 0x70, 0x52, 0x65, 0x71, 0x48, 0x00, 0x52, 0x09, 0x6f, 0x70, 0x65, 0x6e, 0x47, 0x72,
	0x6f, 0x75, 0x70, 0x12, 0x14, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0c, 0x48, 0x00, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x42, 0x09, 0x0a, 0x07, 0x6d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x22, 0x3e, 0x0a, 0x0c, 0x4f, 0x70, 0x65, 0x6e, 0x47, 0x72, 0x6f, 0x75,
	0x70, 0x52, 0x65, 0x71, 0x12, 0x16, 0x0a, 0x06, 0x70, 0x6c, 0x75, 0x67, 0x69, 0x6e, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x70, 0x6c, 0x75, 0x67, 0x69, 0x6e, 0x12, 0x16, 0x0a, 0x06,
	0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x63, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x22, 0x26, 0x0a, 0x10, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x47,
	0x72, 0x6f, 0x75, 0x70, 0x52, 0x65, 0x73, 0x70, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x32, 0x83, 0x01, 0x0a,
	0x06, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x12, 0x79, 0x0a, 0x0c, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x12, 0x30, 0x2e, 0x73, 0x74, 0x61, 0x67, 0x65, 0x64,
	0x70, 0x69, 0x70, 0x65, 0x2e, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x2e, 0x6e, 0x6f, 0x64,
	0x65, 0x73, 0x2e, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x52, 0x65, 0x71, 0x1a, 0x31, 0x2e, 0x73, 0x74, 0x61, 0x67,
	0x65, 0x64, 0x70, 0x69, 0x70, 0x65, 0x2e, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x2e, 0x6e,
	0x6f, 0x64, 0x65, 0x73, 0x2e, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x47, 0x72, 0x6f, 0x75, 0x70, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x28, 0x01,
	0x30, 0x01, 0x42, 0x47, 0x5a, 0x45, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x6a, 0x6f, 0x68, 0x6e, 0x73, 0x69, 0x69, 0x6c, 0x76, 0x65, 0x72, 0x2f, 0x70, 0x69, 0x70,
	0x65, 0x6c, 0x69, 0x6e, 0x65, 0x73, 0x2f, 0x73, 0x74, 0x61, 0x67, 0x65, 0x64, 0x70, 0x69, 0x70,
	0x65, 0x2f, 0x64, 0x69, 0x73, 0x74, 0x72, 0x69, 0x62, 0x2f, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x2f,
	0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescOnce sync.Once
	file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescData = file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDesc
)

func file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescGZIP() []byte {
	file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescOnce.Do(func() {
		file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescData = protoimpl.X.CompressGZIP(file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescData)
	})
	return file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDescData
}

var file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_goTypes = []interface{}{
	(*RequestGroupReq)(nil),  // 0: stagedpipe.distrib.nodes.worker.RequestGroupReq
	(*OpenGroupReq)(nil),     // 1: stagedpipe.distrib.nodes.worker.OpenGroupReq
	(*RequestGroupResp)(nil), // 2: stagedpipe.distrib.nodes.worker.RequestGroupResp
}
var file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_depIdxs = []int32{
	1, // 0: stagedpipe.distrib.nodes.worker.RequestGroupReq.open_group:type_name -> stagedpipe.distrib.nodes.worker.OpenGroupReq
	0, // 1: stagedpipe.distrib.nodes.worker.Worker.RequestGroup:input_type -> stagedpipe.distrib.nodes.worker.RequestGroupReq
	2, // 2: stagedpipe.distrib.nodes.worker.Worker.RequestGroup:output_type -> stagedpipe.distrib.nodes.worker.RequestGroupResp
	2, // [2:3] is the sub-list for method output_type
	1, // [1:2] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_init() }
func file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_init() {
	if File_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestGroupReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OpenGroupReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestGroupResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*RequestGroupReq_OpenGroup)(nil),
		(*RequestGroupReq_Data)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_goTypes,
		DependencyIndexes: file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_depIdxs,
		MessageInfos:      file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_msgTypes,
	}.Build()
	File_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto = out.File
	file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_rawDesc = nil
	file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_goTypes = nil
	file_pipelines_stagedpipe_distrib_nodes_worker_proto_worker_proto_depIdxs = nil
}