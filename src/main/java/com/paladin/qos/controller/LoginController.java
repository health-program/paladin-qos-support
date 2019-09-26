package com.paladin.qos.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import com.paladin.common.specific.CommonUserSession;
import com.paladin.framework.core.session.UserSession;
import com.paladin.framework.web.response.CommonResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api("用户认证模块")
@Controller
@RequestMapping("/qos")
public class LoginController {

	@ApiOperation(value = "后台主页面")
	@GetMapping(value = "/index")
	public Object main(HttpServletRequest request) {
		CommonUserSession userSession = CommonUserSession.getCurrentUserSession();
		ModelAndView model = new ModelAndView("/qos/index");
		model.addObject("name", userSession.getUserName());
		return model;
	}

	@ApiOperation(value = "登录页面")
	@GetMapping("/login")
	public Object loginInput(HttpServletRequest request, HttpServletResponse response) {
		Subject subject = SecurityUtils.getSubject();
		if (subject.isAuthenticated()) {
			return main(request);
		}
		return "/qos/login";
	}

	@ApiOperation(value = "用户认证")
	@RequestMapping(value = "/login", method = RequestMethod.POST, produces = MediaType.TEXT_HTML_VALUE)
	public Object login(HttpServletRequest request, HttpServletResponse response, Model model) {
		Subject subject = SecurityUtils.getSubject();
		if (subject.isAuthenticated()) {
			String username = request.getParameter("username");
			String password = request.getParameter("password");
			if (username != null && username.length() != 0 && password != null && password.length() != 0) {
				subject.login(new UsernamePasswordToken(username, password));
			}
		}

		if (subject.isAuthenticated()) {
			return main(request);
		} else {
			model.addAttribute("isError", true);
			return "/qos/login";
		}
	}

	@ApiOperation(value = "用户认证")
	@RequestMapping(value = "/login", method = RequestMethod.POST)
	@ResponseBody
	public Object ajaxLogin(HttpServletRequest request, HttpServletResponse response, Model model) {
		Subject subject = SecurityUtils.getSubject();
		if (subject.isAuthenticated()) {
			String username = request.getParameter("username");
			String password = request.getParameter("password");
			if (username != null && username.length() != 0 && password != null && password.length() != 0) {
				subject.login(new UsernamePasswordToken(username, password));
			}
		}

		if (subject.isAuthenticated()) {
			return CommonResponse.getSuccessResponse("登录成功", UserSession.getCurrentUserSession().getUserForView());
		} else {
			return CommonResponse.getFailResponse("登录失败");
		}
	}

}
