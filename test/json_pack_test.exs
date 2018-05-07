defmodule JsonPackTest do
  use ExUnit.Case
  doctest JsonPack

  test "greets the world" do
    assert JsonPack.hello() == :world
  end
end
