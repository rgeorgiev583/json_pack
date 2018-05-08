defmodule MsgPackObject do
    defmodule MsgPackExtension do
        @type t :: %__MODULE__{
            type: integer,
            value: binary
        }

        defstruct [
            type: 0,
            value: <<>>
        ]
    end

    @type object_type :: :integer | :nil | :boolean | :float | :string | :binary | :array | :map | :extension
    @type value_type :: integer | nil | boolean | float | binary | [any] | map | MsgPackExtension.t
    @type t :: %__MODULE__{
        type: object_type,
        value: value_type
    }

    defstruct [
        type: :nil,
        value: nil
    ]

    defmodule MsgPackObjectState do
        @type t :: %__MODULE__{
            object: MsgPackObject.t,
            message: binary
        }

        defstruct [
            object: %MsgPackObject{},
            message: <<>>
        ]
    end

    @spec parse_map(message :: binary, size :: integer, map :: map) :: MsgPackObjectState.t
    defp parse_map(message, size, map) do
        if size == 0 do
            %MsgPackObjectState{object: %__MODULE__{type: :map, value: map}, message: message}
        else
            %MsgPackObjectState{object: key_object, message: key_rest} = parse_head(message)
            %MsgPackObjectState{object: value_object, message: value_rest} = parse_head(key_rest)
            updated_map = Map.put(map, key_object, value_object)
            parse_map(value_rest, size - 1, updated_map)
        end
    end

    @spec parse_array(message :: binary, size :: integer, array :: [any]) :: MsgPackObjectState.t
    defp parse_array(message, size, array) do
        if size == 0 do
            %MsgPackObjectState{object: %__MODULE__{type: :array, value: array}, message: message}
        else
            %MsgPackObjectState{object: value_object, message: value_rest} = parse_head(message)
            updated_array = array ++ [value_object]
            parse_array(value_rest, size - 1, updated_array)
        end
    end

    @spec negative_fixint_to_integer(negative_fixint :: bitstring) :: integer
    defp negative_fixint_to_integer(negative_fixint) do
        <<value :: integer - signed - size(8)>> = <<0b111 :: size(3), negative_fixint :: bitstring - size(5)>>
        value
    end

    @spec parse_head(message :: binary) :: MsgPackObjectState.t
    def parse_head(message) do
        case message do
            <<0b0 :: size(1), positive_fixint :: integer - size(7), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: positive_fixint}, message: rest}
            <<0b1000 :: size(4), fixmap_size :: integer - size(4), key_value_pair_description :: binary>> -> parse_map(key_value_pair_description, fixmap_size, %{})
            <<0b1001 :: size(4), fixarray_size :: integer - size(4), value_description :: binary>> -> parse_array(value_description, fixarray_size, [])
            <<0b101 :: size(3), fixstr_size :: integer - size(5), fixstr :: binary - size(fixstr_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :string, value: fixstr}, message: rest}
            <<0b11000000 :: size(8), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :nil, value: nil}, message: rest}
            <<0b11000010 :: size(8), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :boolean, value: false}, message: rest}
            <<0b11000011 :: size(8), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :boolean, value: true}, message: rest}
            <<0b11000100 :: size(8), bin8_size :: integer - size(8), bin8 :: binary - size(bin8_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :binary, value: bin8}, message: rest}
            <<0b11000101 :: size(8), bin16_size :: integer - size(16), bin16 :: binary - size(bin16_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :binary, value: bin16}, message: rest}
            <<0b11000110 :: size(8), bin32_size :: integer - size(32), bin32 :: binary - size(bin32_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :binary, value: bin32}, message: rest}
            <<0b11000111 :: size(8), ext8_size :: integer - size(8), ext8_type :: integer - signed - size(8), ext8 :: binary - size(ext8_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: ext8_type, value: ext8}}, message: rest}
            <<0b11001000 :: size(8), ext16_size :: integer - size(16), ext16_type :: integer - signed - size(8), ext16 :: binary - size(ext16_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: ext16_type, value: ext16}}, message: rest}
            <<0b11001001 :: size(8), ext32_size :: integer - size(32), ext32_type :: integer - signed - size(8), ext32 :: binary - size(ext32_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: ext32_type, value: ext32}}, message: rest}
            <<0b11001010 :: size(8), float32 :: float - size(32), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :float, value: float32}, message: rest}
            <<0b11001011 :: size(8), float64 :: float - size(64), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :float, value: float64}, message: rest}
            <<0b11001100 :: size(8), uint8 :: integer - size(8), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: uint8}, message: rest}
            <<0b11001101 :: size(8), uint16 :: integer - size(16), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: uint16}, message: rest}
            <<0b11001110 :: size(8), uint32 :: integer - size(32), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: uint32}, message: rest}
            <<0b11001111 :: size(8), uint64 :: integer - size(64), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: uint64}, message: rest}
            <<0b11010000 :: size(8), int8 :: integer - signed - size(8), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: int8}, message: rest}
            <<0b11010001 :: size(8), int16 :: integer - signed - size(16), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: int16}, message: rest}
            <<0b11010010 :: size(8), int32 :: integer - signed - size(32), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: int32}, message: rest}
            <<0b11010011 :: size(8), int64 :: integer - signed - size(64), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: int64}, message: rest}
            <<0b11010100 :: size(8), fixext1_type :: integer - signed - size(8), fixext1 :: binary - size(1), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: fixext1_type, value: fixext1}}, message: rest}
            <<0b11010101 :: size(8), fixext2_type :: integer - signed - size(8), fixext2 :: binary - size(2), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: fixext2_type, value: fixext2}}, message: rest}
            <<0b11010110 :: size(8), fixext4_type :: integer - signed - size(8), fixext4 :: binary - size(4), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: fixext4_type, value: fixext4}}, message: rest}
            <<0b11010111 :: size(8), fixext8_type :: integer - signed - size(8), fixext8 :: binary - size(8), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: fixext8_type, value: fixext8}}, message: rest}
            <<0b11011000 :: size(8), fixext16_type :: integer - signed - size(8), fixext16 :: binary - size(16), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :extension, value: %MsgPackExtension{type: fixext16_type, value: fixext16}}, message: rest}
            <<0b11011001 :: size(8), str8_size :: integer - size(8), str8 :: binary - size(str8_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :string, value: str8}, message: rest}
            <<0b11011010 :: size(8), str16_size :: integer - size(16), str16 :: binary - size(str16_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :string, value: str16}, message: rest}
            <<0b11011011 :: size(8), str32_size :: integer - size(32), str32 :: binary - size(str32_size), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :string, value: str32}, message: rest}
            <<0b11011100 :: size(8), array16_size :: integer - size(16), value_description :: binary>> -> parse_array(value_description, array16_size, [])
            <<0b11011101 :: size(8), array32_size :: integer - size(32), value_description :: binary>> -> parse_array(value_description, array32_size, [])
            <<0b11011110 :: size(8), map16_size :: integer - size(16), key_value_pair_description :: binary>> -> parse_map(key_value_pair_description, map16_size, %{})
            <<0b11011111 :: size(8), map32_size :: integer - size(32), key_value_pair_description :: binary>> -> parse_map(key_value_pair_description, map32_size, %{})
            <<0b111 :: size(3), negative_fixint :: bitstring - size(5), rest :: binary>> -> %MsgPackObjectState{object: %__MODULE__{type: :integer, value: negative_fixint_to_integer(negative_fixint)}, message: rest}
        end
    end

    @spec parse(message :: binary) :: t
    def parse(message) do
        %MsgPackObjectState{object: msgpack_object, message: _} = parse_head(message)
        msgpack_object
    end

    @spec serialize_integer(value :: integer) :: binary
    defp serialize_integer(value) do
        case value do
            n when n in 0 .. 127 -> <<value :: integer - size(8)>>
            n when n in 128 .. 255 -> <<0b11001100 :: size(8), value :: integer - size(8)>>
            n when n in 256 .. 65535 -> <<0b11001101 :: size(8), value :: integer - size(16)>>
            n when n in 65536 .. 4294967295 -> <<0b11001110 :: size(8), value :: integer - size(32)>>
            n when n in -32 .. -1 -> <<value :: integer - signed - size(8)>>
            n when n in -128 .. -33 -> <<0b11010000 :: size(8), value :: integer - signed - size(8)>>
            n when n in -32768 .. -129 -> <<0b11010001 :: size(8), value :: integer - signed - size(16)>>
            n when n in -2147483648 .. -32769 -> <<0b11010010 :: size(8), value :: integer - signed - size(32)>>
            n when n >= 4294967296 -> <<0b11001111 :: size(8), value :: integer - size(64)>>
            _ -> <<0b11010011 :: size(8), value :: integer - signed - size(64)>>
        end
    end

    @spec serialize_boolean(value :: boolean) :: binary
    defp serialize_boolean(value) do
        case value do
            false -> <<0b11000010 :: size(8)>>
            true -> <<0b11000011 :: size(8)>>
        end
    end

    @spec serialize_binary(value :: binary) :: binary
    defp serialize_binary(value) do
        case byte_size(value) do
            size when size in 0 .. 255 -> <<0b11000100 :: size(8), size :: integer - size(8), value :: binary - size(size)>>
            size when size in 256 .. 65535 -> <<0b11000101 :: size(8), size :: integer - size(16), value :: binary - size(size)>>
            size -> <<0b11000110 :: size(8), size :: integer - size(32), value :: binary - size(size)>>
        end
    end

    @spec serialize_string(value :: binary) :: binary
    defp serialize_string(value) do
        case byte_size(value) do
            size when size in 0 .. 31 -> <<0b101 :: size(3), size :: integer - size(5), value :: binary - size(size)>>
            size when size in 32 .. 255 -> <<0b11011001 :: size(8), size :: integer - size(8), value :: binary - size(size)>>
            size when size in 256 .. 65535 -> <<0b11011010 :: size(8), size :: integer - size(16), value :: binary - size(size)>>
            size -> <<0b11011011 :: size(8), size :: integer - size(32), value :: binary - size(size)>>
        end
    end

    @spec serialize_binary_or_string(value :: binary) :: binary
    defp serialize_binary_or_string(value) do
        if String.valid?(value) do
            serialize_string(value)
        else
            serialize_binary(value)
        end
    end

    @spec serialize_array(value :: [any]) :: binary
    defp serialize_array(value) do
        header = case Enum.count(value) do
            size when size in 0 .. 15 -> <<0b1001 :: size(4), size :: integer - size(4)>>
            size when size in 16 .. 255 -> <<0b11011100 :: size(8), size :: integer - size(16)>>
            size -> <<0b11011101 :: size(8), size :: integer - size(32)>>
        end
        value_description = value |> Enum.map(&serialize/1) |> Enum.join()
        header <> value_description
    end

    @spec serialize_map(value :: map) :: binary
    defp serialize_map(value) do
        header = case Enum.count(value) do
            size when size in 0 .. 15 -> <<0b1000 :: size(4), size :: integer - size(4)>>
            size when size in 16 .. 255 -> <<0b11011110 :: size(8), size :: integer - size(16)>>
            size -> <<0b11011111 :: size(8), size :: integer - size(32)>>
        end
        key_value_pair_description = value |> Enum.map(fn key_value_pair -> (elem(key_value_pair, 0) |> serialize) <> (elem(key_value_pair, 1) |> serialize) end) |> Enum.join()
        header <> key_value_pair_description
    end

    @spec serialize_extension(type :: integer, value :: binary) :: binary
    defp serialize_extension(type, value) do
        case byte_size(value) do
            1 -> <<0b11010100 :: size(8), type :: integer - signed - size(8), value :: binary - size(1)>>
            2 -> <<0b11010101 :: size(8), type :: integer - signed - size(8), value :: binary - size(2)>>
            4 -> <<0b11010110 :: size(8), type :: integer - signed - size(8), value :: binary - size(4)>>
            8 -> <<0b11010111 :: size(8), type :: integer - signed - size(8), value :: binary - size(8)>>
            16 -> <<0b11011000 :: size(8), type :: integer - signed - size(8), value :: binary - size(16)>>
            size when size in 0 .. 255 -> <<0b11000111 :: size(8), size :: integer - size(8), type :: integer - signed - size(8), value :: binary - size(size)>>
            size when size in 256 .. 65535 -> <<0b11001000 :: size(8), size :: integer - size(16), type :: integer - signed - size(8), value :: binary - size(size)>>
            size -> <<0b11001001 :: size(8), size :: integer - size(32), type :: integer - signed - size(8), value :: binary - size(size)>>
        end
    end

    @spec serialize(value :: value_type) :: binary
    def serialize(value) do
        cond do
            is_integer(value) -> serialize_integer(value)
            is_nil(value) -> <<0b11000000 :: size(8)>>
            is_boolean(value) -> serialize_boolean(value)
            is_float(value) -> <<0b11001011 :: size(8), value :: float - size(64)>>
            is_binary(value) -> serialize_binary_or_string(value)
            is_list(value) -> serialize_array(value)
            is_map(value) -> serialize_map(value)
            %MsgPackExtension{type: ext_type, value: ext_value} = value -> serialize_extension(ext_type, ext_value)
        end
    end
end