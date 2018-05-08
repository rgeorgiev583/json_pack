defmodule MsgPackObjectTest do
    use ExUnit.Case
    doctest MsgPackObject

    test "basic test" do
        message = <<0x82, 0xa7, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x63, 0x74, 0xc3, 0xa6, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x00>>
        object = %{"compact" => true, "schema" => 0}
        assert MsgPackObject.parse(message) |> MsgPackObject.get_bare_object == object
        assert MsgPackObject.serialize(object) == message
    end
end