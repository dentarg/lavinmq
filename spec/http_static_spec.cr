require "./spec_helper"

describe AvalancheMQ::HTTP::StaticController do
  it "GET /" do
    response = ::HTTP::Client.get BASE_URL
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.headers["Content-Length"].to_i.should be > 0
    response.body.should contain("AvalancheMQ")
  end

  it "GET /robots.txt" do
    response = ::HTTP::Client.get "#{BASE_URL}/robots.txt"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/plain")
    response.headers["Content-Length"].to_i.should be > 0
    response.body.should contain("Disallow")
  end

  it "GET /img/favicon.png" do
    response = ::HTTP::Client.get "#{BASE_URL}/img/favicon.png"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("image/png")
    response.headers["Content-Length"].to_i.should be > 0
  end

  it "GET /test/ serves index.html in the directory" do
    response = ::HTTP::Client.get "#{BASE_URL}/test/"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.headers["Content-Length"].to_i.should be > 0
    response.body.should eq("This is a test\n")
  end

  it "GET /test (without trailing /) serves index.html in the directory" do
    response = ::HTTP::Client.get "#{BASE_URL}/test"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.headers["Content-Length"].to_i.should be > 0
    response.body.should eq("This is a test\n")
  end

  it "GET /test/index.html" do
    response = ::HTTP::Client.get "#{BASE_URL}/test/index.html"
    response.status_code.should eq 200
    response.headers["Content-Type"].should contain("text/html")
    response.headers["Content-Length"].to_i.should be > 0
    response.body.should eq("This is a test\n")
  end
end
