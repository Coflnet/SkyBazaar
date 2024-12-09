using Microsoft.AspNetCore.Mvc;
using System;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers
{
    [ApiController]
    public class MigrationController : ControllerBase
    {
        IServiceProvider provider;

        public MigrationController(IServiceProvider provider)
        {
            this.provider = provider;
        }

        [Route("ready")]
        [HttpGet]
        public IActionResult Ready()
        {
            var migrationService = provider.GetService<BazaarService>();
            if(migrationService.LastSuccessfullDB > DateTime.UtcNow - TimeSpan.FromMinutes(5))
            {
                return Ok("success");
            }
            return StatusCode(503);
        }
    }
}
